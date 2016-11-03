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
	DB_API unsigned int __stdcall DbProxy_Start(const char * pCfgFileName = 0);
	DB_API int __stdcall DbProxy_Stop(unsigned int);
	DB_API int __stdcall DbProxy_SetLogType(unsigned int, int);
#ifdef __cplusplus
}
#endif



#endif
