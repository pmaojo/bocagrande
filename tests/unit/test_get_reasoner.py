import sys
import types
import streamlit as st
sys.modules.setdefault("google.generativeai", types.ModuleType("google.generativeai"))
from ui.streamlit_app import get_reasoner
from adapter.hermit_runner import HermiTReasoner


def test_get_reasoner_cached():
    r1 = get_reasoner()
    r2 = get_reasoner()
    assert isinstance(r1, HermiTReasoner)
    assert r1 is r2
