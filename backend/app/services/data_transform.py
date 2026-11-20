"""数据转换编排领域服务：JSON/XML数据转换、字段映射、类型转换与结构重组。

提供从源Schema到目标Schema的字段级映射和值转换能力，
支持嵌套结构抽取、类型强制转换和异常降级策略。
跨模块关联：data_quality_rule（质量校验、规则变更影响对照结果）、
data_lake_sink（转换后落湖）。

待复核清单（transform_review_list）：跨模块读取质量影响明细视图背后的
对照结果，把得到确认的转换范围写入清单，用于安排下一轮转换。
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Callable

from app.persistence import json_store
from app.services.base import BaseModuleService


class DataTransformService(BaseModuleService):
    MODULE = "data_transform"
    ENTITY = "转换规则"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["规则编号", "源Schema", "目标Schema", "字段映射", "转换函数", "默认值填充", "异常策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "源Schema", "目标Schema"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # 待复核清单存储分区与复核状态流转
    REVIEW_KEY = "transform_review_list"
    REVIEW_ACTIONS = {"确认范围": "已确认", "排期转换": "已排期", "退回待复核": "待复核"}

    _BUILTIN_FUNCTIONS: dict[str, Callable[[Any], Any]] = {
        "to_string": lambda x: str(x) if x is not None else "",
        "to_integer": lambda x: int(x) if x is not None else 0,
        "to_float": lambda x: float(x) if x is not None else 0.0,
        "to_boolean": lambda x: bool(x) if x is not None else False,
        "to_upper": lambda x: str(x).upper() if x is not None else "",
        "to_lower": lambda x: str(x).lower() if x is not None else "",
        "extract_numbers": lambda x: re.sub(r"[^0-9]", "", str(x)) if x is not None else "",
        "parse_datetime": lambda x: x,
        "default_empty": lambda x: x if x is not None and str(x).strip() else "",
    }

    def apply_transform(self, rule_id: int, source_data: dict[str, Any]) -> dict[str, Any]:
        """应用转换规则，将源数据转换为目标结构。

        转换流程：
        1. 加载已生效的转换规则
        2. 解析字段映射定义
        3. 逐个字段应用转换函数
        4. 处理异常策略（跳过/默认值/报错）

        Returns:
            { ok: bool, data: dict, errors: list, fields_transformed: int }
        """
        rule = json_store.get_by_id(self.MODULE, rule_id)
        if rule is None:
            return {"ok": False, "data": {}, "errors": ["转换规则不存在"], "fields_transformed": 0}

        if rule.get("status") != "已生效":
            return {"ok": False, "data": {}, "errors": ["转换规则未生效"], "fields_transformed": 0}

        field_mapping = self._parse_mapping(str(rule.get("字段映射", "{}")))
        transform_func = str(rule.get("转换函数", "passthrough"))
        default_filler = str(rule.get("默认值填充", "使用默认值"))
        error_policy = str(rule.get("异常策略", "跳过"))

        result = {}
        errors = []
        transformed = 0

        for target_field, source_path in field_mapping.items():
            try:
                source_value = self._extract_value(source_data, source_path)
                converted = self._apply_function(transform_func, source_value)
                result[target_field] = converted
                transformed += 1
            except (KeyError, ValueError, TypeError) as e:
                if error_policy == "使用默认值":
                    result[target_field] = self._get_default_value(target_field, rule)
                elif error_policy == "报错":
                    errors.append(f"字段 {target_field}: {e}")
                else:
                    errors.append(f"字段 {target_field} 跳过: {e}")

        # 质量校验（跨模块：data_quality_rule）
        quality_check = self._run_quality_checks(rule_id, result)

        return {
            "ok": len(errors) == 0 or error_policy == "跳过",
            "data": result,
            "errors": errors,
            "fields_transformed": transformed,
            "total_fields": len(field_mapping),
            "quality_check": quality_check,
        }

    def _parse_mapping(self, mapping_str: str) -> dict[str, str]:
        """解析字段映射定义。

        支持格式：
        - JSON: {"目标字段": "源字段路径"}
        - 简单: "field1->target1,field2->target2"
        """
        try:
            return json.loads(mapping_str)
        except json.JSONDecodeError:
            result = {}
            for part in mapping_str.split(","):
                if "->" in part:
                    src, tgt = part.split("->", 1)
                    result[tgt.strip()] = src.strip()
            return result if result else {"_raw": "payload"}

    def _extract_value(self, data: dict[str, Any], path: str) -> Any:
        """从嵌套数据中提取指定路径的值。

        支持点号和方括号路径：
        - "name" → data["name"]
        - "user.profile.email" → data["user"]["profile"]["email"]
        - "items[0].id" → data["items"][0]["id"]
        """
        parts = re.split(r"\.|(?<=\w)(?=\[)", path)
        current: Any = data
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if "[" in part and "]" in part:
                key, idx_str = part.split("[", 1)
                idx = int(idx_str.rstrip("]"))
                if key:
                    current = current[key]
                current = current[idx]
            elif isinstance(current, dict):
                current = current.get(part, current.get(path, None))
            else:
                raise KeyError(f"无法从 {type(current)} 中提取 {part}")

        return current

    def _apply_function(self, func_name: str, value: Any) -> Any:
        """应用命名转换函数。"""
        if func_name == "passthrough":
            return value
        func = self._BUILTIN_FUNCTIONS.get(func_name)
        if func:
            return func(value)
        return value

    def _get_default_value(self, field: str, rule: dict[str, Any]) -> Any:
        """根据规则配置获取字段默认值。"""
        default_mapping = {
            "string": "", "integer": 0, "float": 0.0,
            "boolean": False, "datetime": "",
        }
        return default_mapping.get(rule.get("目标Schema", ""), "")

    def _run_quality_checks(self, rule_id: int, transformed_data: dict[str, Any]) -> dict[str, Any]:
        """跨模块运行质量校验。"""
        try:
            from app.services.data_quality_rule import DataQualityRuleService
            quality_svc = DataQualityRuleService()
            return quality_svc.validate_record(transformed_data, source_module=self.MODULE)
        except Exception:
            return {"passed": True, "checks": 0}

    def batch_transform(self, rule_id: int, source_records: list[dict[str, Any]]) -> dict[str, Any]:
        """批量转换多条记录。"""
        results = []
        success = 0
        failed = 0

        for record in source_records:
            r = self.apply_transform(rule_id, record)
            if r["ok"]:
                success += 1
            else:
                failed += 1
            results.append(r)

        return {
            "ok": failed == 0,
            "message": f"批量转换完成: 成功 {success}/{len(source_records)}",
            "success": success,
            "failed": failed,
            "total": len(source_records),
            "results": results,
        }

    # ──────────────────────────────────────────────
    # 待复核清单：跨模块读取质量影响对照结果
    # ──────────────────────────────────────────────
    def import_impact_review_scope(self, report_id: int,
                                   transform_rule_id: int | None = None,
                                   round_label: str = "下一轮") -> dict[str, Any]:
        """把影响报告确认受影响的转换范围写入待复核清单。

        跨模块读取质量影响明细视图背后的对照结果（data_quality_rule），
        新版规则「新增阻断」的来源字段即为需要复核的转换范围；
        同一报告同一来源字段在待复核/已确认阶段不重复写入。
        """
        from app.services.data_quality_rule import DataQualityRuleService
        quality_svc = DataQualityRuleService()
        comparison = quality_svc.impact_comparison_results(report_id)
        if not comparison.get("ok"):
            return {"ok": False, "message": comparison.get("message", "影响报告不存在"),
                    "imported": 0, "items": []}

        report = comparison.get("report") or {}
        impacted = [r for r in comparison.get("results", [])
                    if r.get("影响分类") == "新增阻断"]
        existing = json_store.get_all(self.REVIEW_KEY)

        imported = 0
        skipped = 0
        items: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for row in impacted:
            scope_key = (str(row.get("数据来源")), str(row.get("来源字段")))
            if scope_key in seen:
                skipped += 1
                continue
            seen.add(scope_key)
            duplicated = any(
                e for e in existing
                if int(e.get("来源报告ID", 0)) == int(report_id)
                and str(e.get("数据来源")) == scope_key[0]
                and str(e.get("来源字段")) == scope_key[1]
                and e.get("复核状态") in ("待复核", "已确认")
            )
            if duplicated:
                skipped += 1
                continue
            item = {
                "清单编号": "",
                "来源报告ID": int(report_id),
                "来源报告编号": report.get("报告编号"),
                "关联转换规则ID": int(transform_rule_id) if transform_rule_id else None,
                "数据来源": row.get("数据来源"),
                "来源字段": row.get("来源字段"),
                "影响分类": row.get("影响分类"),
                "转换范围": row.get("来源字段"),
                "复核状态": "待复核",
                "status": "待复核",
                "计划轮次": round_label,
                "pending": True,
                "abnormal": False,
            }
            saved = json_store.insert(self.REVIEW_KEY, item)
            saved = json_store.update(self.REVIEW_KEY, int(saved["id"]),
                                      {"清单编号": f"TRL-{int(saved['id']):04d}"})
            items.append(saved)
            imported += 1

        return {
            "ok": True,
            "report_id": int(report_id),
            "imported": imported,
            "skipped": skipped,
            "items": items,
            "message": f"已把 {imported} 条确认受影响的转换范围写入待复核清单"
                       f"（跳过重复 {skipped} 条），计划轮次：{round_label}",
        }

    def list_review_items(self, status: str | None = None) -> dict[str, Any]:
        """查询待复核清单，可按复核状态过滤。"""
        items = json_store.get_all(self.REVIEW_KEY)
        if status:
            items = [i for i in items if i.get("复核状态") == status]
        items.sort(key=lambda i: int(i.get("id", 0)))
        return {"ok": True, "total": len(items), "items": items}

    def review_action(self, item_id: int, action: str) -> dict[str, Any]:
        """复核动作：确认范围 → 已确认；排期转换 → 已排期（安排下一轮转换）。"""
        item = json_store.get_by_id(self.REVIEW_KEY, int(item_id))
        if item is None:
            return {"ok": False, "message": f"待复核条目 {item_id} 不存在"}
        target = self.REVIEW_ACTIONS.get(action)
        if target is None:
            return {"ok": False,
                    "message": f"动作「{action}」不属于待复核清单可执行范围"}
        saved = json_store.update(self.REVIEW_KEY, int(item_id), {
            "复核状态": target,
            "status": target,
            "pending": target != "已排期",
        })
        return {"ok": True, "item": saved,
                "message": f"待复核条目 {saved.get('清单编号')} 已{action}，状态：{target}"}

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据转换编排的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据转换编排数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "operation": self.OPERATION_TERM,
            "review_items": len(json_store.get_all(self.REVIEW_KEY)),
            "review_pending": sum(
                1 for i in json_store.get_all(self.REVIEW_KEY)
                if i.get("复核状态") == "待复核"),
        }