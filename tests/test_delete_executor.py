from pathlib import Path

import pytest

from atlaspump.delete_executor import CONFIRMATION, permanent_delete, restore, trash


def test_fixture_trash_restore(tmp_path:Path)->None:
 root=tmp_path/'daily';src=root/'d'/'x';src.parent.mkdir(parents=True);src.write_text('x');trashroot=tmp_path/'trash';trashroot.mkdir()
 plan=trash([src],trashroot,allowed_root=root);assert src.exists() and len(plan)==1
 plan=trash([src],trashroot,True,CONFIRMATION,root);target=plan[0][1];assert target.exists() and not src.exists();restore(src,target,root);assert src.exists()
def test_refusals(tmp_path:Path)->None:
 root=tmp_path/'daily';root.mkdir();outside=tmp_path/'x';outside.write_text('x');trashroot=tmp_path/'trash';trashroot.mkdir()
 with pytest.raises(ValueError):trash([outside],trashroot,allowed_root=root)
 with pytest.raises(RuntimeError):permanent_delete()
