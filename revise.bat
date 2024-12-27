@echo off

SET BASEPATH=%~dp0\revise

CALL %JULIA_1112% --project=%BASEPATH% --load=%BASEPATH%\revise.jl
