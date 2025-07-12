import textwrap
from tools.auto_fix import validate_patch


def test_validate_patch_passes_whitelist():
    diff = textwrap.dedent(
        """
        diff --git a/bocagrande/foo.py b/bocagrande/foo.py
        --- a/bocagrande/foo.py
        +++ b/bocagrande/foo.py
        @@
        -a
        +b
        """
    )
    assert validate_patch(diff, whitelist=["bocagrande"], max_size=200)


def test_validate_patch_rejects_large_diff():
    diff = "a" * 101
    assert not validate_patch(diff, whitelist=["bocagrande"], max_size=100)


def test_validate_patch_rejects_outside_whitelist():
    diff = textwrap.dedent(
        """
        diff --git a/other/foo.py b/other/foo.py
        --- a/other/foo.py
        +++ b/other/foo.py
        @@
        -a
        +b
        """
    )
    assert not validate_patch(diff, whitelist=["bocagrande"], max_size=100)

