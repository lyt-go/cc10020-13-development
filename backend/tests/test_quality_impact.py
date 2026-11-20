"""规则变更影响评估（对照样本）业务链路测试。

覆盖链路11：样本补录 → 影响评估 → 质量影响明细视图 → 转换待复核清单。
1. 并发补录同一来源行只取一个生效样本，后来的更正可以回查
2. 影响报告关联实际规则和样本版本
3. 同一评估中迟到更正与待评估同批数据同轮处理
4. 明细视图按来源字段对账新旧规则的通过/阻断样本
5. 数据转换编排跨模块读取对照结果写入待复核清单
"""
from __future__ import annotations

import os
import sys
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app.persistence
import app.services.data_quality_rule as _dq_mod
import app.services.data_transform as _dt_mod
from app.persistence import json_store
from app.services.data_quality_rule import DataQualityRuleService
from app.services.data_transform import DataTransformService

BATCH = "B-20261120-01"
SOURCE = "batch_ingest"
FIELD = "amount"


class TestQualityImpactBase(unittest.TestCase):
    """公共夹具：清空相关分区并准备新旧两版质量规则。"""

    def setUp(self):
        # 其他测试文件的夹具会整体替换 app.persistence.json_store 并重载服务模块，
        # 这里把服务模块的存储引用重新对齐到当前单例，保证用例与执行顺序无关。
        _dq_mod.json_store = app.persistence.json_store
        _dt_mod.json_store = app.persistence.json_store
        self.quality_svc = DataQualityRuleService()
        self.transform_svc = DataTransformService()
        for key in ["data_quality_rule", "quality_sample", "quality_impact_report",
                    "quality_impact_detail", "transform_review_list", "data_transform"]:
            json_store.truncate(key)

        # 旧版规则 v1：非空 + 上界100，阻断
        self.old_rule = json_store.insert("data_quality_rule", {
            "规则编号": "QR-OLD-001",
            "数据来源": "ALL",
            "校验字段": FIELD,
            "校验表达式": "not_empty",
            "阈值下界": "0",
            "阈值上界": "100",
            "阻断策略": "阻断",
            "规则版本": "v1",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })
        # 新版规则 v2：数值 + 上界200，阻断
        self.new_rule = json_store.insert("data_quality_rule", {
            "规则编号": "QR-NEW-002",
            "数据来源": "ALL",
            "校验字段": FIELD,
            "校验表达式": "is_numeric",
            "阈值下界": "0",
            "阈值上界": "200",
            "阻断策略": "阻断",
            "规则版本": "v2",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        })

    def _sample(self, row_key: str, value: str) -> dict:
        return {"样本批次": BATCH, "数据来源": SOURCE,
                "来源行键": row_key, "来源字段": FIELD, "样本值": value}

    def _seed_samples(self) -> None:
        """50/150/250/abc/空：覆盖通过、解除阻断、保持一致、新增阻断。"""
        self.quality_svc.register_samples([
            self._sample("ROW-1", "50"),
            self._sample("ROW-2", "150"),
            self._sample("ROW-3", "250"),
            self._sample("ROW-4", "abc"),
            self._sample("ROW-5", ""),
        ])

    def _create_and_evaluate(self) -> dict:
        created = self.quality_svc.create_impact_report(
            old_rule_id=self.old_rule["id"],
            new_rule_id=self.new_rule["id"],
            sample_batch=BATCH,
        )
        assert created["ok"], created.get("message")
        report_id = int(created["report"]["id"])
        evaluation = self.quality_svc.run_impact_evaluation(report_id)
        return {"report": created["report"], "report_id": report_id,
                "evaluation": evaluation}


class TestSampleRegister(TestQualityImpactBase):
    """样本补录：并发去重、迟到更正、更正链回查。"""

    def test_register_and_deduplicate(self):
        first = self.quality_svc.register_sample(self._sample("ROW-1", "50"))
        self.assertTrue(first["ok"])
        self.assertFalse(first["deduplicated"])

        dup = self.quality_svc.register_sample(self._sample("ROW-1", "50"))
        self.assertTrue(dup["ok"])
        self.assertTrue(dup["deduplicated"])
        self.assertEqual(dup["sample"]["id"], first["sample"]["id"])

        samples = json_store.get_all("quality_sample")
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]["生效标记"], "是")

    def test_late_correction_creates_version_and_keeps_lineage(self):
        first = self.quality_svc.register_sample(self._sample("ROW-1", "50"))
        corrected = self.quality_svc.register_sample(self._sample("ROW-1", "75"))
        self.assertTrue(corrected["ok"])
        self.assertTrue(corrected["corrected"])
        self.assertEqual(corrected["sample"]["样本版本"], 2)
        self.assertEqual(corrected["supersedes"], first["sample"]["id"])

        samples = json_store.get_all("quality_sample")
        self.assertEqual(len(samples), 2)
        effective = [s for s in samples if s.get("生效标记") == "是"]
        self.assertEqual(len(effective), 1)
        self.assertEqual(effective[0]["样本值"], "75")
        old = [s for s in samples if s.get("生效标记") != "是"]
        self.assertEqual(old[0]["status"], "已更正")

        # 后来的更正可以回查
        lineage = self.quality_svc.sample_lineage(corrected["sample"]["id"])
        self.assertTrue(lineage["ok"])
        self.assertEqual(lineage["depth"], 2)
        self.assertEqual([c["样本值"] for c in lineage["chain"]], ["50", "75"])
        self.assertEqual([c["样本版本"] for c in lineage["chain"]], [1, 2])

    def test_concurrent_register_single_effective(self):
        """并发补录同一来源行：只取一个生效样本，全部版本可回查。"""
        n_threads = 8
        barrier = threading.Barrier(n_threads)
        results: list[dict] = []
        lock = threading.Lock()

        def worker(i: int) -> None:
            svc = DataQualityRuleService()
            barrier.wait()
            res = svc.register_sample(self._sample("ROW-C", f"value-{i}"))
            with lock:
                results.append(res)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertTrue(all(r["ok"] for r in results))
        samples = json_store.get_all("quality_sample")
        self.assertEqual(len(samples), n_threads)
        effective = [s for s in samples if s.get("生效标记") == "是"]
        self.assertEqual(len(effective), 1, "并发补录同一来源行只能有一个生效样本")
        versions = sorted(int(s["样本版本"]) for s in samples)
        self.assertEqual(versions, list(range(1, n_threads + 1)))

        lineage = self.quality_svc.sample_lineage(effective[0]["id"])
        self.assertEqual(lineage["depth"], n_threads)


class TestImpactEvaluation(TestQualityImpactBase):
    """影响报告与对照评估。"""

    def test_report_links_actual_rules_and_sample_versions(self):
        self._seed_samples()
        outcome = self._create_and_evaluate()
        report = outcome["report"]

        # 影响报告同时关联实际规则和样本版本
        self.assertEqual(report["旧规则ID"], self.old_rule["id"])
        self.assertEqual(report["旧规则编号"], "QR-OLD-001")
        self.assertEqual(report["旧规则版本"], "v1")
        self.assertEqual(report["新规则ID"], self.new_rule["id"])
        self.assertEqual(report["新规则编号"], "QR-NEW-002")
        self.assertEqual(report["新规则版本"], "v2")

        evaluation = outcome["evaluation"]
        self.assertTrue(evaluation["ok"])
        self.assertEqual(evaluation["status"], "已完成")
        self.assertEqual(evaluation["覆盖样本数"], 5)
        self.assertEqual(evaluation["旧版阻断数"], 3)   # 150、250、空
        self.assertEqual(evaluation["新版阻断数"], 3)   # 250、abc、空
        self.assertEqual(evaluation["新增阻断数"], 1)   # abc
        self.assertEqual(evaluation["解除阻断数"], 1)   # 150

        details = json_store.get_all("quality_impact_detail")
        self.assertEqual(len(details), 5)
        for d in details:
            self.assertEqual(d["样本版本"], 1)
            self.assertEqual(d["报告ID"], outcome["report_id"])

    def test_evaluation_rejects_same_rule(self):
        created = self.quality_svc.create_impact_report(
            old_rule_id=self.old_rule["id"],
            new_rule_id=self.old_rule["id"],
            sample_batch=BATCH,
        )
        self.assertFalse(created["ok"])

    def test_late_correction_and_pending_processed_in_same_run(self):
        """同一评估中：已对照样本收到迟到更正，待评估同批数据同轮处理。"""
        self._seed_samples()
        outcome = self._create_and_evaluate()
        report_id = outcome["report_id"]
        self.assertEqual(outcome["evaluation"]["status"], "已完成")

        # 已对照样本 ROW-4 收到迟到更正（abc → 100）
        corrected = self.quality_svc.register_sample(self._sample("ROW-4", "100"))
        self.assertTrue(corrected["corrected"])
        # 同批新到一条等待评估的数据
        self.quality_svc.register_sample(self._sample("ROW-6", "300"))

        # 样本变化把已完成的报告打回评估中
        report = json_store.get_by_id("quality_impact_report", report_id)
        self.assertEqual(report["评估状态"], "评估中")

        again = self.quality_svc.run_impact_evaluation(report_id)
        self.assertTrue(again["ok"])
        self.assertEqual(again["evaluated"], 2, "迟到更正与待评估样本应同轮处理")
        self.assertEqual(again["reused"], 4)
        self.assertEqual(again["status"], "已完成")
        self.assertEqual(again["覆盖样本数"], 6)
        self.assertEqual(again["新增阻断数"], 0, "更正为 100 后不再新增阻断")

        # 明细只覆盖生效样本：旧版本样本的明细被清理，新版本明细到位
        details = json_store.get_all("quality_impact_detail")
        old_sample_id = corrected["supersedes"]
        self.assertFalse(any(d["样本ID"] == old_sample_id for d in details))
        new_detail = next(d for d in details if d["来源行键"] == "ROW-4")
        self.assertEqual(new_detail["样本版本"], 2)
        self.assertEqual(new_detail["影响分类"], "保持一致")


class TestImpactDetailView(TestQualityImpactBase):
    """质量影响明细视图：覆盖样本及版本、按来源字段对账。"""

    def test_view_lists_covered_samples_and_versions(self):
        self._seed_samples()
        outcome = self._create_and_evaluate()
        view = self.quality_svc.impact_detail_view(outcome["report_id"])

        self.assertTrue(view["ok"])
        self.assertEqual(view["report"]["旧规则版本"], "v1")
        self.assertEqual(view["report"]["新规则版本"], "v2")
        self.assertEqual(len(view["covered_samples"]), 5)
        self.assertEqual(view["pending_samples"], [])
        for covered in view["covered_samples"]:
            self.assertEqual(covered["样本版本"], 1)
        self.assertEqual(view["totals"]["覆盖样本数"], 5)
        self.assertEqual(view["totals"]["待评估样本数"], 0)

    def test_view_reconcile_by_source_field(self):
        self._seed_samples()
        outcome = self._create_and_evaluate()
        view = self.quality_svc.impact_detail_view(outcome["report_id"])

        reconcile = view["reconcile_by_field"]
        self.assertEqual(len(reconcile), 1)
        bucket = reconcile[0]
        self.assertEqual(bucket["来源字段"], FIELD)
        self.assertEqual(bucket["旧版通过"], 2)
        self.assertEqual(bucket["旧版阻断"], 3)
        self.assertEqual(bucket["新版通过"], 2)
        self.assertEqual(bucket["新版阻断"], 3)
        self.assertEqual(bucket["新增阻断"], 1)
        self.assertEqual(bucket["解除阻断"], 1)
        self.assertEqual(len(bucket["旧版阻断样本"]), 3)
        self.assertEqual(len(bucket["新版阻断样本"]), 3)
        self.assertEqual(len(bucket["样本版本"]), 5)

        # 对账：分桶计数必须与明细逐条一致
        details = view["details"]
        self.assertEqual(
            bucket["旧版阻断"],
            sum(1 for d in details if d["旧规则结果"] == "阻断"),
        )
        self.assertEqual(
            bucket["新版阻断"],
            sum(1 for d in details if d["新规则结果"] == "阻断"),
        )

    def test_view_shows_pending_before_evaluation(self):
        self._seed_samples()
        created = self.quality_svc.create_impact_report(
            old_rule_id=self.old_rule["id"],
            new_rule_id=self.new_rule["id"],
            sample_batch=BATCH,
        )
        view = self.quality_svc.impact_detail_view(int(created["report"]["id"]))
        self.assertEqual(len(view["covered_samples"]), 0)
        self.assertEqual(len(view["pending_samples"]), 5,
                         "等待评估的同批数据应在视图中列出")


class TestTransformReviewList(TestQualityImpactBase):
    """数据转换编排跨模块读取对照结果，写入待复核清单。"""

    def test_import_scope_and_review_flow(self):
        self._seed_samples()
        outcome = self._create_and_evaluate()

        imported = self.transform_svc.import_impact_review_scope(
            report_id=outcome["report_id"], transform_rule_id=None,
            round_label="第2轮")
        self.assertTrue(imported["ok"])
        self.assertEqual(imported["imported"], 1, "新增阻断字段应进入待复核清单")
        item = imported["items"][0]
        self.assertEqual(item["来源字段"], FIELD)
        self.assertEqual(item["影响分类"], "新增阻断")
        self.assertEqual(item["复核状态"], "待复核")
        self.assertEqual(item["计划轮次"], "第2轮")
        self.assertEqual(item["来源报告编号"], outcome["report"]["报告编号"])

        # 重复导入不重复写入
        again = self.transform_svc.import_impact_review_scope(
            report_id=outcome["report_id"])
        self.assertEqual(again["imported"], 0)
        self.assertEqual(again["skipped"], 1)

        # 复核流转：确认范围 → 排期转换（安排下一轮转换）
        confirmed = self.transform_svc.review_action(item["id"], "确认范围")
        self.assertTrue(confirmed["ok"])
        self.assertEqual(confirmed["item"]["复核状态"], "已确认")
        scheduled = self.transform_svc.review_action(item["id"], "排期转换")
        self.assertEqual(scheduled["item"]["复核状态"], "已排期")

        listing = self.transform_svc.list_review_items(status="已排期")
        self.assertEqual(listing["total"], 1)

    def test_import_scope_follows_late_correction(self):
        """迟到更正改变对照结果后，转换范围以最新评估为准。"""
        self._seed_samples()
        outcome = self._create_and_evaluate()
        report_id = outcome["report_id"]

        # 迟到更正消除新增阻断后重新评估
        self.quality_svc.register_sample(self._sample("ROW-4", "100"))
        self.quality_svc.run_impact_evaluation(report_id)

        imported = self.transform_svc.import_impact_review_scope(report_id=report_id)
        self.assertTrue(imported["ok"])
        self.assertEqual(imported["imported"], 0, "对照结果无新增阻断则不写入清单")

    def test_import_missing_report(self):
        result = self.transform_svc.import_impact_review_scope(report_id=99999)
        self.assertFalse(result["ok"])

    def test_comparison_results_are_cross_module_readable(self):
        self._seed_samples()
        outcome = self._create_and_evaluate()
        comparison = self.quality_svc.impact_comparison_results(outcome["report_id"])
        self.assertTrue(comparison["ok"])
        self.assertEqual(len(comparison["results"]), 5)
        self.assertEqual(comparison["report"]["报告编号"],
                         outcome["report"]["报告编号"])


class TestImpactSnapshots(TestQualityImpactBase):
    """领域快照纳入样本、报告与待复核清单统计。"""

    def test_snapshots(self):
        self._seed_samples()
        outcome = self._create_and_evaluate()
        self.transform_svc.import_impact_review_scope(report_id=outcome["report_id"])

        quality_snapshot = self.quality_svc.domain_snapshot()
        self.assertEqual(quality_snapshot["module"], "data_quality_rule")
        self.assertEqual(quality_snapshot["samples"], 5)
        self.assertEqual(quality_snapshot["effective_samples"], 5)
        self.assertEqual(quality_snapshot["impact_reports"], 1)

        transform_snapshot = self.transform_svc.domain_snapshot()
        self.assertEqual(transform_snapshot["module"], "data_transform")
        self.assertEqual(transform_snapshot["review_items"], 1)
        self.assertEqual(transform_snapshot["review_pending"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
