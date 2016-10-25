#include <stdio.h>
#include <string.h>
#include <Windows.h>
#include "EscortDbProxy.h"

int main(int argc, char ** argv)
{
	char szCfgFile[256] = { 0 };
	unsigned int uiInst = 0;
	if (argc == 3 && (strcmp(argv[1], "-l") == 0)) {	
		sprintf_s(szCfgFile, sizeof(szCfgFile), "%s", argv[2]);
	}
	if (strlen(szCfgFile)) {
		uiInst = DbProxy_Start();
	}
	else {
		uiInst = DbProxy_Start(szCfgFile);
	}
	DWORD dwProcessId = GetCurrentProcessId();
	printf("PID=%lu, DbProxy Instance=%u\n", dwProcessId, uiInst);
	getchar();
	if (uiInst) {
		DbProxy_Stop(uiInst);
	}
	printf("end\n");
	return 0;
}