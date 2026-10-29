"""Temporal boundaries and atomic publication without fitting Spark models."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from . import training


class Expression:
    def __init__(self,function): self.function=function
    def __add__(self,value): return Expression(lambda row:self.function(row)+value)
    def __lt__(self,value): return Expression(lambda row:self.function(row)<value)
    def __ge__(self,value): return Expression(lambda row:self.function(row)>=value)


class Frame:
    def __init__(self,rows): self.rows=rows
    def filter(self,expression): return Frame([r for r in self.rows if expression.function(r)])


class CalibrationBoundaryTests(unittest.TestCase):
    def test_future_labels_cannot_reach_selection_period(self):
        boundary=training.CALIBRATION_END
        times=[boundary-2700000,boundary-1800001,boundary-1800000,
               boundary-900000,boundary-1,boundary,boundary+900000]
        frame=Frame([{"open_ms":timestamp} for timestamp in times])
        functions=SimpleNamespace(col=lambda key:Expression(lambda row:row[key]))
        with patch.object(training,"F",functions):
            calibration,selection=training.calibration_splits(frame)
        self.assertEqual([times[0],times[1]],[r["open_ms"] for r in calibration.rows])
        self.assertEqual([boundary,boundary+900000],[r["open_ms"] for r in selection.rows])
        self.assertTrue(all(row["open_ms"]+1800000<boundary for row in calibration.rows))
        self.assertFalse(set(r["open_ms"] for r in calibration.rows)&set(r["open_ms"] for r in selection.rows))


class RecalibrationPublicationTests(unittest.TestCase):
    def base_registry(self):
        return {"model_version":"base","models":{
            "volatility":{"selected":"random_forest","model_path":"models/base/volatility","test_deployed":{"rmse":0.2}},
            "divergence":{"selected":"baseline","model_path":"models/base/divergence","test_deployed":{"rmse":0.3}},
            "crash":{"selected":"logistic_regression","model_path":"models/base/crash","test_deployed":{"brier":0.1}},
            "anomaly":{"selected":"logistic_regression","model_path":"models/base/anomaly","test_deployed":{"brier":0.2}}}}

    def run_mock(self,root,base,fail=False):
        (root/"model_dataset").mkdir()
        (root/"model_dataset"/"_SUCCESS").touch()
        (root/"model_registry.json").write_text(json.dumps(base))
        spark=SimpleNamespace(read=SimpleNamespace(parquet=lambda _:"unchanged_dataset"),stop=lambda:None)
        saves=[]
        calls=[]
        def classifier(data,target,name,version,prefit=None):
            calls.append((data,target,name,prefit))
            if fail and name=="anomaly": raise RuntimeError("calibration failed")
            return {**prefit,"calibration_embargo_minutes":30,"base_model_reused":True}
        def save(path,value): saves.append((path,value))
        with patch.object(training,"ML",root),patch.object(training,"get_spark",return_value=spark), \
             patch.object(training,"train_classifier",side_effect=classifier),patch.object(training,"save_json",side_effect=save), \
             patch.object(training,"status",side_effect=lambda session,**details:{"session":session,**details}), \
             patch.object(training,"prepare",side_effect=AssertionError("prepare must not run")), \
             patch.object(training,"train_regression",side_effect=AssertionError("regression must not refit")):
            if fail:
                with self.assertRaisesRegex(RuntimeError,"calibration failed"):
                    training.recalibrate()
            else:
                training.recalibrate()
        return saves,calls

    def test_reuses_models_preserves_regression_and_publishes_registry_last(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            base=self.base_registry()
            saves,calls=self.run_mock(root,base)
            self.assertEqual(["crash","anomaly"],[call[2] for call in calls])
            self.assertTrue(all(call[0]=="unchanged_dataset" for call in calls))
            self.assertEqual(root/"model_registry.json",saves[-1][0])
            registry=saves[-1][1]
            self.assertEqual("base",registry["base_model_version"])
            self.assertTrue(registry["model_version"].endswith(".cal"))
            for name in ("volatility","divergence"):
                self.assertEqual(base["models"][name],registry["models"][name])
            for name in ("crash","anomaly"):
                self.assertEqual(base["models"][name]["model_path"],registry["models"][name]["model_path"])

    def test_failure_before_both_classifiers_does_not_publish_partial_registry(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            base=self.base_registry()
            saves,_=self.run_mock(root,base,fail=True)
            self.assertFalse(any(path==root/"model_registry.json" for path,_ in saves))
            self.assertEqual(base,json.loads((root/"model_registry.json").read_text()))


if __name__=="__main__":
    unittest.main()
