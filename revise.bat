@echo off

SET BASEPATH=%~dp0\revise

%JULIA_1100% --project=%BASEPATH% --load=%BASEPATH%\revise.jl
