/**
Copyright 2019 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

#include "Python.h"
#include "Python_C_API.h"

void Python_C_API::train(int argc, char *argv[]) {

    if (argc%2 != 0) {
        fprintf(stderr, "Usage: [--flag name] [args]\n");
    }

    Py_Initialize();
    FILE* file;
    wchar_t* _argv[argc];
    for(int i=0; i<argc; i++){
        wchar_t *arg = Py_DecodeLocale(argv[i], NULL);
        _argv[i] = arg;
    }

    PySys_SetArgv(argc, _argv);
    PyObject *sys = PyImport_ImportModule("sys");
    PyObject *path = PyObject_GetAttrString(sys, "path");

    PyList_Append(path, PyUnicode_FromString("."));
    file = fopen("./test.py","r");
    PyRun_SimpleFile(file, "./test.py");
    fclose(file);
    Py_Finalize();
}
