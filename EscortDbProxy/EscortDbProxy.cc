#include "EscortDbProxy.h"

#include <Windows.h>

BOOL APIENTRY DllMain(void * hInst, unsigned long ulReason, void * pReserved)
{
	switch (ulReason) {
		case DLL_PROCESS_ATTACH: {
			break;
		}
		case DLL_PROCESS_DETACH: {
			break;
		}
		case DLL_THREAD_ATTACH: {
			break;
		}
		case DLL_THREAD_DETACH: {
			break;
		}
	}
	return TRUE;
}

DB_API int __stdcall DbProxy_Start()
{

}

DB_API int __stdcall DbProxy_Stop()
{

}

DB_API int __stdcall DbProxy_Query()
{

}

DB_API int __stdcall DbProxy_ExecSQL()
{

}