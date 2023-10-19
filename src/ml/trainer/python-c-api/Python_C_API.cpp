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

#include "Python_C_API.h"

#include "Python.h"

using namespace std;

void Python_C_API::train(int argc, char *argv[]) {
    if (argc % 2 != 0) {
        fprintf(stderr, "Usage: [--flag name] [args]\n");
    }
    FILE *file;
    wchar_t *program = Py_DecodeLocale(argv[0], NULL);
    wchar_t **_argv;
    for (int i = 0; i < argc; i++) {
        wchar_t *arg = Py_DecodeLocale(argv[i], NULL);
        _argv[i] = arg;
    }

    Py_Initialize();
    PySys_SetArgv(argc, _argv);
    file = fopen("./GraphSAGE/graphsage/unsupervised_train.py", "r");
    PyRun_SimpleFile(file, "./GraphSAGE/graphsage/unsupervised_train.py");
    Py_Finalize();
}

int Python_C_API::predict(int argc, char *argv[]) {
    PyObject *pName, *pModule, *pFunc;
    PyObject *pArgs, *pValue;
    int i;

    if (argc != 4) {
        fprintf(stderr, "Usage: call [args]\n");
        return 1;
    }

    Py_Initialize();
    PyObject *sys = PyImport_ImportModule("sys");
    PyObject *path = PyObject_GetAttrString(sys, "path");
    PyList_Append(path, PyUnicode_FromString("./GraphSAGE/graphsage/"));
    pName = PyUnicode_DecodeFSDefault("predict");
    pModule = PyImport_Import(pName);
    Py_DECREF(pName);

    if (pModule != NULL) {
        pFunc = PyObject_GetAttrString(pModule, "predict");
        /* pFunc is a new reference */

        if (pFunc && PyCallable_Check(pFunc)) {
            pArgs = PyTuple_New(argc);
            for (i = 0; i < argc; ++i) {
                pValue = PyUnicode_FromString(argv[i]);
                if (!pValue) {
                    Py_DECREF(pArgs);
                    Py_DECREF(pModule);
                    fprintf(stderr, "Cannot convert argument\n");
                    return 1;
                }
                /* pValue reference stolen here: */
                PyTuple_SetItem(pArgs, i, pValue);
            }
            PyObject_CallObject(pFunc, pArgs);
            Py_DECREF(pArgs);
        } else {
            if (PyErr_Occurred()) PyErr_Print();
            fprintf(stderr, "Cannot find function \"%s\"\n", "predict");
        }
        Py_XDECREF(pFunc);
        Py_DECREF(pModule);
    } else {
        PyErr_Print();
        fprintf(stderr, "Failed to load \"%s\"\n", "predict");
        return 1;
    }
    Py_Finalize();
    return 0;
}
