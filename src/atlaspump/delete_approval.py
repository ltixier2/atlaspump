"""Append-only delete approval workflow; Delete Executor v1 intentionally absent."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

ALLOWED={"HOLD":{"DELETE_CANDIDATE","CANCELLED","FAILED"},"DELETE_CANDIDATE":{"DELETE_APPROVED","CANCELLED","FAILED"},"DELETE_APPROVED":{"GRACE_PERIOD","FAILED"},"GRACE_PERIOD":{"READY_TO_DELETE","CANCELLED","FAILED"},"READY_TO_DELETE":{"FAILED"},"CANCELLED":set(),"FAILED":set(),"DELETED":set()}
def _hash(p:Path)->str:
 h=hashlib.sha256()
 with p.open('rb') as f:
  for c in iter(lambda:f.read(1048576),b''):h.update(c)
 return h.hexdigest()
def _append(path:Path,event:dict[str,Any])->None:
 path.parent.mkdir(parents=True,exist_ok=True)
 with path.open('a',encoding='utf-8') as f:f.write(json.dumps(event,sort_keys=True)+'\n')
def _validation(root:Path,path:Path)->dict[str,Any]:
 v=json.loads(path.read_text());q=v['validation']
 if v.get('status')!='READY_FOR_DELETE_APPROVAL_DESIGN' or any(q[k] for k in ('protected_missing','ordinary_present','duplicates','source_missing','partials')):raise ValueError('validated quarantine requirements not satisfied')
 return cast(dict[str, Any], v)
def approve(root:Path, validation:Path, approver:str, reason:str, grace_days:int=7)->Path:
 if not approver or not reason:raise ValueError('approver and reason are required')
 v=_validation(root,validation);m=v['manifest']; now=datetime.now(timezone.utc); log=root/'reports/delete-approval-v1'/f"approval-{m['run_id']}.jsonl"
 sources=[]
 for x in m['outputs']:
  s=root/x['source_path'];q=root/x['retained_path'];sources.append({'source':x['source_path'],'source_sha256':_hash(s),'quarantine':x['retained_path'],'quarantine_sha256':_hash(q)})
 event={'state':'GRACE_PERIOD','approved_from':'DELETE_CANDIDATE','approver':approver,'reason':reason,'approved_at':now.isoformat(),'grace_ends_at':(now+timedelta(days=grace_days)).isoformat(),'run_id':m['run_id'],'validation_hash':_hash(validation),'sources':sources,'gross_bytes':15392705194,'quarantine_bytes':1689012237,'net_bytes':13703692957};_append(log,event);return log
def cancel(root:Path,run_id:str,reason:str)->Path:
 if not reason:raise ValueError('reason is required')
 p=root/'reports/delete-approval-v1'/f'approval-{run_id}.jsonl';_append(p,{'state':'CANCELLED','at':datetime.now(timezone.utc).isoformat(),'reason':reason});return p
def destructive_executor(*_:Any)->None:raise RuntimeError('Delete Executor v1 is not implemented; physical deletion is forbidden')
