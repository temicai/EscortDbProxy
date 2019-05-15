#include <stdio.h>
#include <string.h>
#include <Windows.h>
#include "EscortDbProxy.h"
#include "WatchDog.h"

bool authorize()
{
	if (VerifyKeyFile() == 0) {
		return true;
	}
	return false;
}

int main(int argc, char ** argv)
{
	if (!authorize()) {
		printf("authorize failed\n");
		return 0;
	}
	char szCfgFile[256] = { 0 };
	unsigned long long ullInst = 0;
	if (argc == 3 && (strcmp(argv[1], "-l") == 0)) {	
		sprintf_s(szCfgFile, sizeof(szCfgFile), "%s", argv[2]);
	}
	if (strlen(szCfgFile)) {
		ullInst = DbProxy_Start();
	}
	else {
		ullInst = DbProxy_Start(szCfgFile);
	}
	if (ullInst > 0) {
		DWORD dwProcessId = GetCurrentProcessId();
		printf("PID=%lu, DbProxy Instance=%llu\n", dwProcessId, ullInst);
		while (1) {
#ifdef _DEBUG
			char c = 0;
			scanf_s("%c", &c, 1);
			if (c == 'q') {
				break;
			}
#endif
			Sleep(500);
		}
		if (ullInst) {
			DbProxy_Stop(ullInst);
		}
	}
	printf("end\n");
	return 0;
}