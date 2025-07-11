
import logging

from app.pentaho_importer.transformation_model import (
    TransformationModel,
    Step,
    Hop,
)



def test_get_execution_order_simple():
    model = TransformationModel()
    step1 = Step(name="A", step_type="start")
    step2 = Step(name="B", step_type="middle")
    step3 = Step(name="C", step_type="end")
    model.add_step(step1)
    model.add_step(step2)
    model.add_step(step3)
    model.add_hop(Hop("A", "B"))
    model.add_hop(Hop("B", "C"))
    assert [s.name for s in model.get_execution_order()] == ["A", "B", "C"]


def test_get_execution_order_skips_disabled_hops():
    model = TransformationModel()
    step1 = Step(name="A", step_type="start")
    step2 = Step(name="B", step_type="middle")
    step3 = Step(name="C", step_type="end")
    model.add_step(step1)
    model.add_step(step2)
    model.add_step(step3)
    model.add_hop(Hop("A", "B", enabled=False))
    model.add_hop(Hop("A", "C"))
    # With the hop to B disabled, both B and C have no dependencies.
    assert [s.name for s in model.get_execution_order()] == ["A", "B", "C"]


def test_get_execution_order_logs_warning(caplog):
    model = TransformationModel()
    step1 = Step(name="A", step_type="start")
    step2 = Step(name="B", step_type="middle")
    step3 = Step(name="C", step_type="end")
    step4 = Step(name="D", step_type="orphan")
    model.add_step(step1)
    model.add_step(step2)
    model.add_step(step3)
    model.add_step(step4)
    model.add_hop(Hop("A", "B"))
    model.add_hop(Hop("B", "C"))
    with caplog.at_level(logging.INFO):
        order = model.get_execution_order()
    assert [s.name for s in order if s.name != "D"] == ["A", "B", "C"]
    assert "Topological sort might be incomplete" in caplog.text

