#ifndef ESCORTDBPROXY_H
#define ESCORTDBPROXY_H

#ifdef __cplusplus
extern "C"
{
#endif

#ifdef DLL_IMPORT
#define DB_API __declspec(dllimport)
#else 
#define DB_API __declspec(dllexport)
#endif
	DB_API int __stdcall DbProxy_Start();
	DB_API int __stdcall DbProxy_Stop();
	DB_API int __stdcall DbProxy_Query();
	DB_API int __stdcall DbProxy_ExecSQL();
#ifdef __cplusplus
}
#endif



#endif
