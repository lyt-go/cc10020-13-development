"""接入质量校验领域服务：字段级校验、完整性检查、阈值告警与阻断策略。

提供转换后数据的质量校验能力，包括必填字段检查、类型校验、
数值范围校验和完整性评分。
跨模块关联：data_transform（转换后校验）、batch_ingest（批量质量检查）。

规则变更影响评估（对照样本）：
- quality_sample：同批字段样本，同一来源行只保留一个生效样本，
  迟到更正生成新版本，旧版本保留可回查。
- quality_impact_report：影响报告，关联实际规则（ID+编号+版本+快照）
  与样本批次。
- quality_impact_detail：新旧规则逐样本对照明细，是质量影响明细
  视图背后的对照结果，供 data_transform 跨模块读取。
"""
from __future__ import annotations

import re
import threading
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService

# 样本补录的读改写必须整体串行，否则并发补录同一来源行会出现多个生效样本。
_SAMPLE_LOCK = threading.Lock()


class DataQualityRuleService(BaseModuleService):
    MODULE = "data_quality_rule"
    ENTITY = "质量规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "数据来源", "校验字段", "校验表达式", "阈值下界", "阈值上界", "阻断策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "数据来源", "校验字段"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # 规则变更影响评估使用的存储分区
    SAMPLE_KEY = "quality_sample"
    REPORT_KEY = "quality_impact_report"
    DETAIL_KEY = "quality_impact_detail"

    def __init__(self) -> None:
        super().__init__()
        self._validation_history: list[dict[str, Any]] = []

    def validate_record(self, record: dict[str, Any], source_module: str = "") -> dict[str, Any]:
        """对单条记录执行质量校验。

        校验流程：
        1. 加载与该来源相关的已生效规则
        2. 逐条规则执行校验表达式
        3. 检查数值范围（阈值上下界）
        4. 根据阻断策略决定是否拒绝

        Returns:
            { passed: bool, checks: int, failed: int, details: list, score: int }
        """
        rules = json_store.get_all(self.MODULE)
        matching = [
            r for r in rules
            if r.get("status") == "已生效"
            and (str(r.get("数据来源", "")) in (source_module, "", "ALL", "*")
                 or source_module == ""
                 or not source_module)
        ]

        if not matching:
            return {"passed": True, "checks": 0, "failed": 0, "details": [], "score": 100}

        results = []
        total_passed = 0
        total_failed = 0

        for rule in matching:
            field = str(rule.get("校验字段", ""))
            expression = str(rule.get("校验表达式", ""))
            block_policy = str(rule.get("阻断策略", "警告"))
            try:
                lower = float(rule.get("阈值下界", "0") or "0")
                upper = float(rule.get("阈值上界", "0") or "0")
            except ValueError:
                lower = 0
                upper = 0

            check_result = self._execute_check(record, field, expression)
            range_check = self._check_range(record, field, lower, upper)

            passed = check_result.get("passed", True) and range_check.get("passed", True)
            detail = {
                "rule_no": rule.get("规则编号"),
                "field": field,
                "expression": expression,
                "passed": passed,
                "expr_result": check_result,
                "range_result": range_check,
                "block_policy": block_policy,
            }
            results.append(detail)

            if passed:
                total_passed += 1
            else:
                total_failed += 1
                if block_policy == "阻断":
                    self._validation_history.append({
                        "rule": rule.get("规则编号"),
                        "field": field,
                        "outcome": "blocked",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                    return {
                        "passed": False,
                        "checks": len(matching),
                        "failed": total_failed,
                        "passed_checks": total_passed,
                        "details": results,
                        "score": max(0, 100 - total_failed * 10),
                        "blocked": True,
                    }

        self._validation_history.append({
            "record_preview": str(record)[:100],
            "checks": len(matching),
            "passed": total_passed,
            "failed": total_failed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "passed": total_failed == 0,
            "checks": len(matching),
            "failed": total_failed,
            "passed_checks": total_passed,
            "details": results,
            "score": max(0, 100 - total_failed * 10),
        }

    def _execute_check(self, record: dict[str, Any], field: str, expression: str) -> dict[str, Any]:
        """执行校验表达式。

        支持的表达式操作：
        - "not_empty": 非空检查
        - "is_numeric": 是否为数字
        - "pattern:xxx": 正则匹配
        - "in:val1,val2": 值域检查
        - "len_gt:N": 长度大于N
        """
        value = record.get(field)
        field_present = field in record

        if expression == "not_empty":
            if not field_present or value is None or str(value).strip() == "":
                return {"passed": False, "message": f"字段 {field} 为空"}
            return {"passed": True, "message": "通过"}

        if expression == "is_numeric":
            try:
                float(str(value))
                return {"passed": True, "message": "通过"}
            except (ValueError, TypeError):
                return {"passed": False, "message": f"字段 {field} 不是数字"}

        if expression.startswith("pattern:"):
            pattern = expression.split(":", 1)[1]
            if not re.match(pattern, str(value)):
                return {"passed": False, "message": f"字段 {field} 不匹配模式 {pattern}"}
            return {"passed": True, "message": "通过"}

        if expression.startswith("in:"):
            allowed = expression.split(":", 1)[1].split(",")
            if str(value) not in allowed:
                return {"passed": False, "message": f"字段 {field} 不在允许值 {allowed} 中"}
            return {"passed": True, "message": "通过"}

        if expression.startswith("len_gt:"):
            min_len = int(expression.split(":", 1)[1])
            if len(str(value or "")) < min_len:
                return {"passed": False, "message": f"字段 {field} 长度小于 {min_len}"}
            return {"passed": True, "message": "通过"}

        return {"passed": True, "message": "未识别表达式，默认通过"}

    def _check_range(self, record: dict[str, Any], field: str, lower: float, upper: float) -> dict[str, Any]:
        """检查字段值是否在阈值范围内。"""
        if lower == 0 and upper == 0:
            return {"passed": True, "message": "无范围限制"}

        value = record.get(field)
        try:
            num_value = float(str(value))
        except (ValueError, TypeError):
            return {"passed": True, "message": "非数值字段跳过范围检查"}

        if lower > 0 and num_value < lower:
            return {"passed": False, "message": f"值 {num_value} 低于阈值 {lower}"}
        if upper > 0 and num_value > upper:
            return {"passed": False, "message": f"值 {num_value} 超过阈值 {upper}"}

        return {"passed": True, "message": f"值 {num_value} 在范围 [{lower}, {upper}] 内"}

    def validate_batch(self, records: list[dict[str, Any]], source_module: str = "") -> dict[str, Any]:
        """批量质量校验。"""
        results = []
        passed_count = 0
        failed_count = 0
        blocked_count = 0

        for record in records:
            r = self.validate_record(record, source_module)
            if r.get("blocked"):
                blocked_count += 1
                failed_count += 1
            elif r["passed"]:
                passed_count += 1
            else:
                failed_count += 1
            results.append(r)

        avg_score = sum(r.get("score", 0) for r in results) / max(len(results), 1)

        return {
            "ok": blocked_count == 0,
            "total": len(records),
            "passed": passed_count,
            "failed": failed_count,
            "blocked": blocked_count,
            "average_score": round(avg_score, 2),
            "results": results,
        }

    def get_quality_report(self) -> dict[str, Any]:
        """获取质量校验报告。"""
        history = self._validation_history[-100:]
        rules = json_store.get_all(self.MODULE)
        active_rules = sum(1 for r in rules if r.get("status") == "已生效")

        return {
            "active_rules": active_rules,
            "total_checks": len(history),
            "recent_failures": sum(1 for h in history if h.get("failed", 0) > 0),
            "recent_blocks": sum(1 for h in history if h.get("blocked")),
        }

    # ──────────────────────────────────────────────
    # 对照样本：补录、去重、迟到更正与回查
    # ──────────────────────────────────────────────
    def register_sample(self, values: dict[str, Any]) -> dict[str, Any]:
        """补录一条字段样本。

        同一来源行（样本批次+数据来源+来源行键+来源字段）只保留一个生效样本：
        - 值相同的并发补录直接去重，返回已生效样本；
        - 值不同的补录视为迟到更正，旧版本标记「已更正」保留可回查，
          新版本成为唯一生效样本，评估状态重置为「待评估」。
        """
        required = ["样本批次", "数据来源", "来源行键", "来源字段"]
        missing = [f for f in required if not str(values.get(f) or "").strip()]
        if missing:
            return {"ok": False, "message": f"缺少必填字段：{'、'.join(missing)}"}
        if "样本值" not in values:
            return {"ok": False, "message": "缺少必填字段：样本值"}

        batch = str(values["样本批次"]).strip()
        source = str(values["数据来源"]).strip()
        row_key = str(values["来源行键"]).strip()
        field = str(values["来源字段"]).strip()
        value = values.get("样本值")

        with _SAMPLE_LOCK:
            samples = json_store.get_all(self.SAMPLE_KEY)
            current = next(
                (s for s in samples
                 if str(s.get("样本批次")) == batch
                 and str(s.get("数据来源")) == source
                 and str(s.get("来源行键")) == row_key
                 and str(s.get("来源字段")) == field
                 and s.get("生效标记") == "是"),
                None,
            )
            if current is not None and str(current.get("样本值")) == str(value):
                return {
                    "ok": True,
                    "deduplicated": True,
                    "corrected": False,
                    "sample": current,
                    "message": "同一来源行已存在生效样本，并发补录只取一个",
                }

            if current is not None:
                json_store.update(self.SAMPLE_KEY, int(current["id"]),
                                  {"生效标记": "否", "status": "已更正"})
            version = int(current.get("样本版本", 0)) + 1 if current else 1
            record = {
                "样本编号": "",
                "样本批次": batch,
                "数据来源": source,
                "来源行键": row_key,
                "来源字段": field,
                "样本值": value,
                "样本版本": version,
                "生效标记": "是",
                "更正自": int(current["id"]) if current else None,
                "评估状态": "待评估",
                "status": "生效",
                "pending": True,
                "abnormal": False,
            }
            saved = json_store.insert(self.SAMPLE_KEY, record)
            saved = json_store.update(self.SAMPLE_KEY, int(saved["id"]),
                                      {"样本编号": f"QS-{int(saved['id']):04d}"})
            self._touch_reports_for_batch(batch)

        return {
            "ok": True,
            "deduplicated": False,
            "corrected": current is not None,
            "supersedes": int(current["id"]) if current else None,
            "sample": saved,
            "message": "样本已补录" if current is None
                       else f"迟到更正已生效，样本版本升至 v{version}，旧版本可回查",
        }

    def register_samples(self, samples: list[dict[str, Any]]) -> dict[str, Any]:
        """批量补录同批字段样本。"""
        results = [self.register_sample(s) for s in samples]
        failed = [r for r in results if not r.get("ok")]
        return {
            "ok": not failed,
            "total": len(samples),
            "inserted": sum(1 for r in results if r.get("ok") and not r.get("deduplicated") and not r.get("corrected")),
            "corrected": sum(1 for r in results if r.get("corrected")),
            "deduplicated": sum(1 for r in results if r.get("deduplicated")),
            "failed": len(failed),
            "results": results,
        }

    def sample_lineage(self, sample_id: int) -> dict[str, Any]:
        """回查样本更正链：从最初版本到当前生效版本的完整链路。"""
        samples = json_store.get_all(self.SAMPLE_KEY)
        by_id = {int(s["id"]): s for s in samples}
        target = by_id.get(int(sample_id))
        if target is None:
            return {"ok": False, "message": f"样本 {sample_id} 不存在", "chain": []}

        chain = [target]
        cursor = target
        while cursor.get("更正自") is not None and int(cursor["更正自"]) in by_id:
            cursor = by_id[int(cursor["更正自"])]
            chain.insert(0, cursor)
        cursor = target
        while True:
            nxt = next(
                (s for s in samples
                 if s.get("更正自") is not None and int(s["更正自"]) == int(cursor["id"])),
                None,
            )
            if nxt is None:
                break
            chain.append(nxt)
            cursor = nxt

        return {
            "ok": True,
            "sample_id": int(sample_id),
            "depth": len(chain),
            "chain": [
                {
                    "id": int(s["id"]),
                    "样本编号": s.get("样本编号"),
                    "样本版本": s.get("样本版本"),
                    "样本值": s.get("样本值"),
                    "生效标记": s.get("生效标记"),
                    "status": s.get("status"),
                    "评估状态": s.get("评估状态"),
                    "updated_at": s.get("updated_at"),
                }
                for s in chain
            ],
        }

    def _touch_reports_for_batch(self, batch: str) -> None:
        """同批样本发生变化（新补录/迟到更正）时，把已完成的影响报告打回评估中。"""
        for report in json_store.get_all(self.REPORT_KEY):
            if str(report.get("样本批次")) == batch and report.get("评估状态") == "已完成":
                json_store.update(self.REPORT_KEY, int(report["id"]),
                                  {"评估状态": "评估中", "status": "评估中", "pending": True})

    # ──────────────────────────────────────────────
    # 影响报告：关联实际规则与样本版本
    # ──────────────────────────────────────────────
    def create_impact_report(self, old_rule_id: int, new_rule_id: int,
                             sample_batch: str) -> dict[str, Any]:
        """创建规则变更影响报告。

        报告关联实际规则（ID+编号+版本），并快照评估所需的规则配置，
        保证同一报告的评估结果可复现；样本侧通过明细记录样本版本。
        """
        if int(old_rule_id) == int(new_rule_id):
            return {"ok": False, "message": "旧版规则与新版规则不能是同一条"}
        rules = json_store.get_all(self.MODULE)
        old_rule = next((r for r in rules if int(r.get("id", 0)) == int(old_rule_id)), None)
        new_rule = next((r for r in rules if int(r.get("id", 0)) == int(new_rule_id)), None)
        if old_rule is None:
            return {"ok": False, "message": f"旧版规则 {old_rule_id} 不存在"}
        if new_rule is None:
            return {"ok": False, "message": f"新版规则 {new_rule_id} 不存在"}

        report = {
            "报告编号": "",
            "样本批次": str(sample_batch).strip(),
            "旧规则ID": int(old_rule_id),
            "旧规则编号": old_rule.get("规则编号"),
            "旧规则版本": old_rule.get("规则版本"),
            "新规则ID": int(new_rule_id),
            "新规则编号": new_rule.get("规则编号"),
            "新规则版本": new_rule.get("规则版本"),
            "旧规则快照": dict(old_rule),
            "新规则快照": dict(new_rule),
            "覆盖样本数": 0,
            "旧版阻断数": 0,
            "新版阻断数": 0,
            "新增阻断数": 0,
            "解除阻断数": 0,
            "评估状态": "评估中",
            "status": "评估中",
            "pending": True,
            "abnormal": False,
        }
        saved = json_store.insert(self.REPORT_KEY, report)
        saved = json_store.update(self.REPORT_KEY, int(saved["id"]),
                                  {"报告编号": f"QIR-{int(saved['id']):04d}"})
        return {"ok": True, "report": saved,
                "message": f"影响报告 {saved['报告编号']} 已创建，关联规则 "
                           f"{saved['旧规则编号']}({saved['旧规则版本']}) → "
                           f"{saved['新规则编号']}({saved['新规则版本']})"}

    # ──────────────────────────────────────────────
    # 影响评估：逐生效样本对照新旧规则
    # ──────────────────────────────────────────────
    def _evaluate_rule_against_sample(self, rule: dict[str, Any],
                                      sample: dict[str, Any]) -> dict[str, Any]:
        """用一条规则评估一条字段样本，返回 通过/阻断/未适用。"""
        rule_field = str(rule.get("校验字段", ""))
        rule_source = str(rule.get("数据来源", ""))
        sample_source = str(sample.get("数据来源", ""))
        if rule_field != str(sample.get("来源字段", "")):
            return {"结果": "未适用",
                    "说明": f"规则校验字段 {rule_field} 与样本字段 {sample.get('来源字段')} 不一致"}
        if rule_source not in ("", "ALL", "*") and rule_source != sample_source:
            return {"结果": "未适用",
                    "说明": f"规则数据来源 {rule_source} 不覆盖样本来源 {sample_source}"}

        record = {rule_field: sample.get("样本值")}
        expression = str(rule.get("校验表达式", ""))
        try:
            lower = float(rule.get("阈值下界", "0") or "0")
            upper = float(rule.get("阈值上界", "0") or "0")
        except ValueError:
            lower = upper = 0

        check = self._execute_check(record, rule_field, expression)
        range_check = self._check_range(record, rule_field, lower, upper)
        passed = check.get("passed", True) and range_check.get("passed", True)
        policy = str(rule.get("阻断策略", "警告"))

        if passed:
            return {"结果": "通过", "说明": "校验通过"}
        if policy == "阻断":
            return {"结果": "阻断",
                    "说明": f"触发阻断：{check.get('message')}；{range_check.get('message')}"}
        return {"结果": "通过",
                "说明": f"校验未通过但策略为{policy}，放行：{check.get('message')}"}

    @staticmethod
    def _classify_impact(old_result: str, new_result: str) -> str:
        if "未适用" in (old_result, new_result):
            return "未适用"
        if old_result == "阻断" and new_result == "通过":
            return "解除阻断"
        if old_result == "通过" and new_result == "阻断":
            return "新增阻断"
        return "保持一致"

    def run_impact_evaluation(self, report_id: int) -> dict[str, Any]:
        """执行一次影响评估。

        同一评估中：
        - 已对照且未更正的样本直接复用既有明细；
        - 收到迟到更正的样本（生效版本前进）按新版本重新对照；
        - 等待评估的同批样本在同一轮处理完毕。
        """
        report = json_store.get_by_id(self.REPORT_KEY, int(report_id))
        if report is None:
            return {"ok": False, "message": f"影响报告 {report_id} 不存在"}
        batch = str(report.get("样本批次", ""))
        old_rule = report.get("旧规则快照") or {}
        new_rule = report.get("新规则快照") or {}

        samples = [s for s in json_store.get_all(self.SAMPLE_KEY)
                   if str(s.get("样本批次")) == batch and s.get("生效标记") == "是"]
        effective_ids = {int(s["id"]) for s in samples}

        details = [d for d in json_store.get_all(self.DETAIL_KEY)
                   if int(d.get("报告ID", 0)) == int(report_id)]
        # 清理已失效样本（被迟到更正取代）留下的旧明细，保证报告只覆盖生效样本
        for detail in details:
            if int(detail.get("样本ID", 0)) not in effective_ids:
                json_store.delete(self.DETAIL_KEY, int(detail["id"]))
        details = [d for d in details if int(d.get("样本ID", 0)) in effective_ids]
        detail_by_sample = {int(d["样本ID"]): d for d in details}

        evaluated = 0
        reused = 0
        now = datetime.now(timezone.utc).isoformat()
        for sample in samples:
            sample_id = int(sample["id"])
            existing = detail_by_sample.get(sample_id)
            if (existing is not None
                    and int(existing.get("样本版本", 0)) == int(sample.get("样本版本", 0))
                    and sample.get("评估状态") == "已对照"):
                reused += 1
                continue

            old_result = self._evaluate_rule_against_sample(old_rule, sample)
            new_result = self._evaluate_rule_against_sample(new_rule, sample)
            payload = {
                "报告ID": int(report_id),
                "样本ID": sample_id,
                "样本编号": sample.get("样本编号"),
                "样本版本": sample.get("样本版本"),
                "样本批次": batch,
                "数据来源": sample.get("数据来源"),
                "来源行键": sample.get("来源行键"),
                "来源字段": sample.get("来源字段"),
                "样本值": sample.get("样本值"),
                "旧规则结果": old_result["结果"],
                "旧规则说明": old_result["说明"],
                "新规则结果": new_result["结果"],
                "新规则说明": new_result["说明"],
                "影响分类": self._classify_impact(old_result["结果"], new_result["结果"]),
                "评估时间": now,
            }
            if existing is not None:
                json_store.update(self.DETAIL_KEY, int(existing["id"]), payload)
            else:
                json_store.insert(self.DETAIL_KEY, payload)
            json_store.update(self.SAMPLE_KEY, sample_id, {"评估状态": "已对照"})
            evaluated += 1

        final_details = [d for d in json_store.get_all(self.DETAIL_KEY)
                         if int(d.get("报告ID", 0)) == int(report_id)]
        pending_left = [s for s in json_store.get_all(self.SAMPLE_KEY)
                        if str(s.get("样本批次")) == batch
                        and s.get("生效标记") == "是"
                        and s.get("评估状态") != "已对照"]
        status = "评估中" if pending_left else "已完成"
        stats = {
            "覆盖样本数": len(final_details),
            "旧版阻断数": sum(1 for d in final_details if d.get("旧规则结果") == "阻断"),
            "新版阻断数": sum(1 for d in final_details if d.get("新规则结果") == "阻断"),
            "新增阻断数": sum(1 for d in final_details if d.get("影响分类") == "新增阻断"),
            "解除阻断数": sum(1 for d in final_details if d.get("影响分类") == "解除阻断"),
        }
        json_store.update(self.REPORT_KEY, int(report_id),
                          {**stats, "评估状态": status, "status": status,
                           "pending": status != "已完成"})

        return {
            "ok": True,
            "report_id": int(report_id),
            "evaluated": evaluated,
            "reused": reused,
            "pending_remaining": len(pending_left),
            "status": status,
            **stats,
            "message": f"评估完成：新对照 {evaluated} 条，复用 {reused} 条，"
                       f"待评估剩余 {len(pending_left)} 条",
        }

    # ──────────────────────────────────────────────
    # 质量影响明细视图与其背后的对照结果
    # ──────────────────────────────────────────────
    def impact_comparison_results(self, report_id: int) -> dict[str, Any]:
        """返回明细视图背后的对照结果，供 data_transform 等模块跨模块读取。"""
        report = json_store.get_by_id(self.REPORT_KEY, int(report_id))
        if report is None:
            return {"ok": False, "message": f"影响报告 {report_id} 不存在", "results": []}
        details = [d for d in json_store.get_all(self.DETAIL_KEY)
                   if int(d.get("报告ID", 0)) == int(report_id)]
        details.sort(key=lambda d: (str(d.get("来源字段")), str(d.get("来源行键"))))
        return {"ok": True, "report": report, "results": details}

    def impact_detail_view(self, report_id: int) -> dict[str, Any]:
        """质量影响明细视图。

        列出报告已覆盖的样本及版本，并按来源字段对账新旧规则的
        通过/阻断样本；同时给出同批仍在等待评估的样本。
        """
        report = json_store.get_by_id(self.REPORT_KEY, int(report_id))
        if report is None:
            return {"ok": False, "message": f"影响报告 {report_id} 不存在"}
        details = [d for d in json_store.get_all(self.DETAIL_KEY)
                   if int(d.get("报告ID", 0)) == int(report_id)]
        details.sort(key=lambda d: (str(d.get("来源字段")), str(d.get("来源行键"))))

        covered_samples = [
            {
                "样本ID": int(d["样本ID"]),
                "样本编号": d.get("样本编号"),
                "样本版本": d.get("样本版本"),
                "数据来源": d.get("数据来源"),
                "来源行键": d.get("来源行键"),
                "来源字段": d.get("来源字段"),
            }
            for d in details
        ]
        pending_samples = [
            {
                "样本ID": int(s["id"]),
                "样本编号": s.get("样本编号"),
                "样本版本": s.get("样本版本"),
                "来源行键": s.get("来源行键"),
                "来源字段": s.get("来源字段"),
                "评估状态": s.get("评估状态"),
            }
            for s in json_store.get_all(self.SAMPLE_KEY)
            if str(s.get("样本批次")) == str(report.get("样本批次"))
            and s.get("生效标记") == "是"
            and s.get("评估状态") != "已对照"
        ]

        reconcile: dict[str, dict[str, Any]] = {}
        for d in details:
            field = str(d.get("来源字段"))
            bucket = reconcile.setdefault(field, {
                "来源字段": field,
                "旧版通过": 0, "旧版阻断": 0,
                "新版通过": 0, "新版阻断": 0,
                "新增阻断": 0, "解除阻断": 0, "未适用": 0,
                "旧版阻断样本": [], "新版阻断样本": [],
                "样本版本": {},
            })
            if d.get("旧规则结果") == "阻断":
                bucket["旧版阻断"] += 1
                bucket["旧版阻断样本"].append(d.get("样本编号"))
            elif d.get("旧规则结果") == "通过":
                bucket["旧版通过"] += 1
            if d.get("新规则结果") == "阻断":
                bucket["新版阻断"] += 1
                bucket["新版阻断样本"].append(d.get("样本编号"))
            elif d.get("新规则结果") == "通过":
                bucket["新版通过"] += 1
            impact = str(d.get("影响分类"))
            if impact in ("新增阻断", "解除阻断", "未适用"):
                bucket[impact] += 1
            bucket["样本版本"][str(d.get("样本编号"))] = d.get("样本版本")

        return {
            "ok": True,
            "report": {
                "id": int(report["id"]),
                "报告编号": report.get("报告编号"),
                "样本批次": report.get("样本批次"),
                "旧规则ID": report.get("旧规则ID"),
                "旧规则编号": report.get("旧规则编号"),
                "旧规则版本": report.get("旧规则版本"),
                "新规则ID": report.get("新规则ID"),
                "新规则编号": report.get("新规则编号"),
                "新规则版本": report.get("新规则版本"),
                "评估状态": report.get("评估状态"),
            },
            "covered_samples": covered_samples,
            "pending_samples": pending_samples,
            "details": details,
            "reconcile_by_field": [reconcile[f] for f in sorted(reconcile)],
            "totals": {
                "覆盖样本数": len(covered_samples),
                "待评估样本数": len(pending_samples),
                "旧版阻断数": sum(1 for d in details if d.get("旧规则结果") == "阻断"),
                "新版阻断数": sum(1 for d in details if d.get("新规则结果") == "阻断"),
                "新增阻断数": sum(1 for d in details if d.get("影响分类") == "新增阻断"),
                "解除阻断数": sum(1 for d in details if d.get("影响分类") == "解除阻断"),
            },
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出接入质量校验的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "接入质量校验数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_validations": len(self._validation_history),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "operation": self.OPERATION_TERM,
            "samples": len(json_store.get_all(self.SAMPLE_KEY)),
            "effective_samples": sum(
                1 for s in json_store.get_all(self.SAMPLE_KEY)
                if s.get("生效标记") == "是"),
            "impact_reports": len(json_store.get_all(self.REPORT_KEY)),
        }