Set fso = CreateObject("Scripting.FileSystemObject")
scriptDir = fso.GetParentFolderName(WScript.ScriptFullName)
cmd = "pythonw """ & scriptDir & "\watch_tsv_to_json_bg.py""" 
CreateObject("WScript.Shell").Run cmd, 0, False
