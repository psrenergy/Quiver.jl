@echo off

SET BASEPATH=%~dp0

CALL "%JULIA_1112%" --project=%BASEPATH% --interactive --load=%BASEPATH%\revise.jl
