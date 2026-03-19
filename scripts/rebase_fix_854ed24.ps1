# 作為 GIT_SEQUENCE_EDITOR：把 pick 854ed24 改成 edit 854ed24
$todoFile = $args[1]
if (-not $todoFile) { $todoFile = $args[0] }
(Get-Content $todoFile -Raw) -replace '(?m)^pick 854ed24\b', 'edit 854ed24' | Set-Content $todoFile -NoNewline:$false
