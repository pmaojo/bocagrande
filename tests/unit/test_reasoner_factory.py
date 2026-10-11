import pytest

from adapter.hermit_runner import HermiTReasoner
from adapter.ontologos_reasoner import OntoLogosReasoner
from adapter.reasoner_factory import DEFAULT_REASONER, create_reasoner


def test_default_reasoner_needs_no_java():
    assert DEFAULT_REASONER == "ontologos"
    assert isinstance(create_reasoner(), OntoLogosReasoner)


def test_hermit_and_none_and_unknown():
    assert isinstance(create_reasoner("hermit", hermit_jar="x.jar"), HermiTReasoner)
    assert create_reasoner("none") is None
    with pytest.raises(ValueError, match="Unknown reasoner"):
        create_reasoner("pellet")
