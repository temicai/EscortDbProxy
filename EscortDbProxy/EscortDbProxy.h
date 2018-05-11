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
	unsigned long long __stdcall DbProxy_Start(const char * pCfgFileName = 0);
	int __stdcall DbProxy_Stop(unsigned long long);
	int __stdcall DbProxy_SetLogType(unsigned long long, unsigned short);
#ifdef __cplusplus
}
#endif



#endif
