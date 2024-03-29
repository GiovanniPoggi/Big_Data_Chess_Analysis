@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  BDE-mr-surname1-surname2 startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Add default JVM options here. You can also use JAVA_OPTS and BDE_MR_SURNAME1_SURNAME2_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto init

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:init
@rem Get command-line arguments, handling Windows variants

if not "%OS%" == "Windows_NT" goto win9xME_args

:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\BDE-mr-surname1-surname2.jar;%APP_HOME%\lib\commons-math3-3.6.1.jar;%APP_HOME%\lib\hadoop-common-2.6.0-cdh5.13.1.jar;%APP_HOME%\lib\hadoop-core-2.6.0-mr1-cdh5.13.1.jar;%APP_HOME%\lib\hadoop-hdfs-2.6.0-cdh5.13.1.jar;%APP_HOME%\lib\guava-21.0.jar;%APP_HOME%\lib\hadoop-annotations-2.6.0-cdh5.13.1.jar;%APP_HOME%\lib\commons-cli-1.2.jar;%APP_HOME%\lib\xmlenc-0.52.jar;%APP_HOME%\lib\commons-httpclient-3.1.jar;%APP_HOME%\lib\commons-io-2.4.jar;%APP_HOME%\lib\commons-net-3.1.jar;%APP_HOME%\lib\commons-collections-3.2.2.jar;%APP_HOME%\lib\servlet-api-2.5.jar;%APP_HOME%\lib\jetty-6.1.26.cloudera.4.jar;%APP_HOME%\lib\jetty-util-6.1.26.cloudera.4.jar;%APP_HOME%\lib\jersey-core-1.9.jar;%APP_HOME%\lib\jersey-json-1.9.jar;%APP_HOME%\lib\jersey-server-1.9.jar;%APP_HOME%\lib\jasper-compiler-5.5.23.jar;%APP_HOME%\lib\jasper-runtime-5.5.23.jar;%APP_HOME%\lib\jsp-api-2.1.jar;%APP_HOME%\lib\commons-el-1.0.jar;%APP_HOME%\lib\commons-logging-1.1.3.jar;%APP_HOME%\lib\log4j-1.2.17.jar;%APP_HOME%\lib\jets3t-0.9.0.jar;%APP_HOME%\lib\commons-lang-2.6.jar;%APP_HOME%\lib\commons-configuration-1.6.jar;%APP_HOME%\lib\slf4j-log4j12-1.7.5.jar;%APP_HOME%\lib\avro-1.7.6-cdh5.13.1.jar;%APP_HOME%\lib\protobuf-java-2.5.0.jar;%APP_HOME%\lib\gson-2.2.4.jar;%APP_HOME%\lib\hadoop-auth-2.6.0-cdh5.13.1.jar;%APP_HOME%\lib\jsch-0.1.42.jar;%APP_HOME%\lib\curator-client-2.7.1.jar;%APP_HOME%\lib\curator-recipes-2.7.1.jar;%APP_HOME%\lib\jsr305-3.0.0.jar;%APP_HOME%\lib\htrace-core4-4.0.1-incubating.jar;%APP_HOME%\lib\commons-compress-1.4.1.jar;%APP_HOME%\lib\hsqldb-1.8.0.10.jar;%APP_HOME%\lib\core-3.1.1.jar;%APP_HOME%\lib\commons-daemon-1.0.13.jar;%APP_HOME%\lib\netty-3.10.5.Final.jar;%APP_HOME%\lib\netty-all-4.0.23.Final.jar;%APP_HOME%\lib\xercesImpl-2.9.1.jar;%APP_HOME%\lib\leveldbjni-all-1.8.jar;%APP_HOME%\lib\jettison-1.1.jar;%APP_HOME%\lib\jaxb-impl-2.2.3-1.jar;%APP_HOME%\lib\jackson-jaxrs-1.8.3.jar;%APP_HOME%\lib\jackson-xc-1.8.3.jar;%APP_HOME%\lib\asm-3.1.jar;%APP_HOME%\lib\commons-digester-1.8.jar;%APP_HOME%\lib\commons-beanutils-core-1.8.0.jar;%APP_HOME%\lib\paranamer-2.3.jar;%APP_HOME%\lib\snappy-java-1.0.4.1.jar;%APP_HOME%\lib\httpclient-4.2.5.jar;%APP_HOME%\lib\apacheds-kerberos-codec-2.0.0-M15.jar;%APP_HOME%\lib\curator-framework-2.7.1.jar;%APP_HOME%\lib\xz-1.0.jar;%APP_HOME%\lib\jaxb-api-2.2.2.jar;%APP_HOME%\lib\commons-beanutils-1.7.0.jar;%APP_HOME%\lib\httpcore-4.2.4.jar;%APP_HOME%\lib\apacheds-i18n-2.0.0-M15.jar;%APP_HOME%\lib\api-asn1-api-1.0.0-M20.jar;%APP_HOME%\lib\api-util-1.0.0-M20.jar;%APP_HOME%\lib\stax-api-1.0-2.jar;%APP_HOME%\lib\activation-1.1.jar;%APP_HOME%\lib\java-xmlbuilder-0.4.jar;%APP_HOME%\lib\jackson-core-asl-1.9.13.jar;%APP_HOME%\lib\jackson-mapper-asl-1.9.13.jar;%APP_HOME%\lib\commons-codec-1.6.jar;%APP_HOME%\lib\slf4j-api-1.7.6.jar;%APP_HOME%\lib\zookeeper-3.4.6.jar;%APP_HOME%\lib\jline-0.9.94.jar;%APP_HOME%\lib\junit-3.8.1.jar

@rem Execute BDE-mr-surname1-surname2
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %BDE_MR_SURNAME1_SURNAME2_OPTS%  -classpath "%CLASSPATH%" ChessDriver.java %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

:fail
rem Set variable BDE_MR_SURNAME1_SURNAME2_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%BDE_MR_SURNAME1_SURNAME2_EXIT_CONSOLE%" exit 1
exit /b 1

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
