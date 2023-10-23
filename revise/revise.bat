@echo off

SET BASEPATH=%~dp0

%JULIA_185% --project=%BASEPATH% --load=%BASEPATH%\revise.jl
