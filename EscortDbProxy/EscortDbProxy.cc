#include "EscortDbProxy.h"
#include "DbProxyConcrete.h"
#include <Windows.h>
#include <string>
#include <fstream>
#include <map>

typedef std::map<std::string, std::string> KVStringPair;

static char g_szDllDir[256] = { 0 };
std::map<unsigned long long, DbProxy *> g_instList;
pthread_mutex_t g_mutex4InstList;

int loadConf(const char *, KVStringPair &);
char * readItem(KVStringPair, const char *);

BOOL APIENTRY DllMain(void * hInst, unsigned long ulReason, void * pReserved)
{
	switch (ulReason) {
		case DLL_PROCESS_ATTACH: {
			pthread_mutex_init(&g_mutex4InstList, NULL);
			g_instList.clear();
			g_szDllDir[0] = '\0';
			char szPath[256] = { 0 };
			if (GetModuleFileNameA((HMODULE)hInst, szPath, sizeof(szPath)) != 0) {
				char drive[32] = { 0 };
				char dir[256] = { 0 };
				_splitpath_s(szPath, drive, sizeof(drive), dir, sizeof(dir), NULL, 0, NULL, 0);
				snprintf(g_szDllDir, sizeof(g_szDllDir), "%s%s", drive, dir);
			}
			break;
		}
		case DLL_PROCESS_DETACH: {
			pthread_mutex_lock(&g_mutex4InstList);
			if (!g_instList.empty()) {
				std::map<unsigned long long, DbProxy *>::iterator iter = g_instList.begin();
				do {
					DbProxy * pProxy = iter->second;
					if (pProxy) {
						if (pProxy->GetState()) {
							pProxy->Stop();
						}
						delete pProxy;
						pProxy = NULL;
					}
					iter = g_instList.erase(iter);
				} while (iter != g_instList.end());
			}
			pthread_mutex_unlock(&g_mutex4InstList);
			pthread_mutex_destroy(&g_mutex4InstList);
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

int loadConf(const char * pFileName_, KVStringPair & kvList_)
{
	int result = -1;
	std::fstream cfgFile;
	char buffer[256] = { 0 };
	cfgFile.open(pFileName_, std::ios::in);
	if (cfgFile.is_open()) {
		while (!cfgFile.eof()) {
			cfgFile.getline(buffer, 256, '\n');
			std::string str = buffer;
			if (str[0] == '#') { //comment line
				continue;
			}
			size_t n = str.find_first_of('=');
			if (n != std::string::npos) {
				std::string keyStr = str.substr(0, n);
				std::string valueStr = str.substr(n + 1);
				kvList_.insert(std::make_pair(keyStr, valueStr));
				result = 0;
			}
		}
	}
	cfgFile.close();
	return result;
}

char * readItem(KVStringPair kvList_, const char * pItem_)
{
	if (!kvList_.empty()) {
		if (pItem_ && strlen(pItem_)) {
			KVStringPair::iterator iter = kvList_.find(pItem_);
			if (iter != kvList_.end()) {
				std::string strValue = iter->second;
				size_t nSize = strValue.size();
				if (nSize) {
					char * value = (char *)malloc(nSize + 1);
					strncpy_s(value, nSize + 1, strValue.c_str(), nSize);
					value[nSize] = '\0';
					return value;
				}
			}
		}
	}
	return NULL;
}

unsigned long long __stdcall DbProxy_Start(const char * pCfgFileName_)
{
	unsigned long long result = 0;
	if (strlen(g_szDllDir)) {
		char szFileName[256] = { 0 };
		if (pCfgFileName_ && strlen(pCfgFileName_)) {
			strncpy_s(szFileName, sizeof(szFileName), pCfgFileName_, strlen(pCfgFileName_));
		}
		else {
			snprintf(szFileName, sizeof(szFileName), "%sconf\\server.data", g_szDllDir);
		}
		KVStringPair kvList;
		if (loadConf(szFileName, kvList) == 0) {
			char * pZkHost = readItem(kvList, "zk_host");
			char * pMidwareHost = readItem(kvList, "midware_ip");
			char * pMidwarePubPort = readItem(kvList, "publish_port");
			char * pMidwareTalkPort = readItem(kvList, "talk_port");
			char * pMidwareCollectPort = readItem(kvList, "collect_port");
			char * pDbProxyHost = readItem(kvList, "db_proxy_ip");
			char * pDbProxyQryPort = readItem(kvList, "query_port");
			char * pDbHost = readItem(kvList, "db_host_ip");
			char * pDbUser = readItem(kvList, "db_user");
			char * pDbPasswd = readItem(kvList, "db_passwd");
			char * pDbMajorSample = readItem(kvList, "db_major_sample");
			char * pDbLocateSample = readItem(kvList, "db_locate_sample");
			char * pDbUsePort = readItem(kvList, "db_port");
			char * pSlaveDbHost = readItem(kvList, "db_slave_host_ip");
			char * pSlaveDbUser = readItem(kvList, "db_slave_user");
			char * pSlaveDbPasswd = readItem(kvList, "db_slave_passwd");
			char * pSlaveDbPort = readItem(kvList, "db_slave_port");
			char szZkHost[256] = { 0 };
			char szMidwareHost[32] = { 0 };
			unsigned short usMidwarePublishPort = 0;
			unsigned short usMidwareTalkPort = 0;
			unsigned short usMidwareCollectPort = 0;
			char szDbProxyHost[32] = { 0 };
			unsigned short usDbProxyQryPort = 0;
			char szMasterDbHost[32] = { 0 };
			char szMasterDbUser[32] = { 0 };
			char szMasterDbPasswd[64] = { 0 };
			unsigned short usMasterDbPort = 0;
			char szSlaveDbHost[32] = { 0 };
			char szSlaveDbUser[32] = { 0 };
			char szSlaveDbPasswd[64] = { 0 };
			unsigned short usSlaveDbPort = 0;
			char szDbMajorSample[32] = { 0 };
			char szDbLocateSample[32] = { 0 };
			if (pZkHost) {
				strncpy_s(szZkHost, sizeof(szZkHost), pZkHost, strlen(pZkHost));
				free(pZkHost);
				pZkHost = NULL;
			}
			if (pMidwareHost) {
				strncpy_s(szMidwareHost, sizeof(szMidwareHost), pMidwareHost, strlen(pMidwareHost));
				free(pMidwareHost);
				pMidwareHost = NULL;
			}
			if (pMidwarePubPort) {
				usMidwarePublishPort = (unsigned short)atoi(pMidwarePubPort);
				free(pMidwarePubPort);
				pMidwarePubPort = NULL;
			}
			if (pMidwareTalkPort) {
				usMidwareTalkPort = (unsigned short)atoi(pMidwareTalkPort);
				free(pMidwareTalkPort);
				pMidwareTalkPort = NULL;
			}
			if (pMidwareCollectPort) {
				usMidwareCollectPort = (unsigned short)atoi(pMidwareCollectPort);
				free(pMidwareCollectPort);
				pMidwareCollectPort = NULL;
			}
			if (pDbProxyHost) {
				strncpy_s(szDbProxyHost, sizeof(szDbProxyHost), pDbProxyHost, strlen(pDbProxyHost));
				free(pDbProxyHost);
				pDbProxyHost = NULL;
			}
			if (pDbProxyQryPort) {
				usDbProxyQryPort = (unsigned short)atoi(pDbProxyQryPort);
				free(pDbProxyQryPort);
				pDbProxyQryPort = NULL;
			}
			if (pDbHost) {
				strncpy_s(szMasterDbHost, sizeof(szMasterDbHost), pDbHost, strlen(pDbHost));
				free(pDbHost);
				pDbHost = NULL;
			}
			if (pDbUser) {
				strncpy_s(szMasterDbUser, sizeof(szMasterDbUser), pDbUser, strlen(pDbUser));
				free(pDbUser);
				pDbUser = NULL;
			}
			if (pDbPasswd) {
				strncpy_s(szMasterDbPasswd, sizeof(szMasterDbPasswd), pDbPasswd, strlen(pDbPasswd));
				free(pDbPasswd);
				pDbPasswd = NULL;
			}
			if (pDbMajorSample) {
				strncpy_s(szDbMajorSample, sizeof(szDbMajorSample), pDbMajorSample, strlen(pDbMajorSample));
				free(pDbMajorSample);
				pDbMajorSample = NULL;
			}
			if (pDbLocateSample) {
				strncpy_s(szDbLocateSample, sizeof(szDbLocateSample), pDbLocateSample, strlen(pDbLocateSample));
				free(pDbLocateSample);
				pDbLocateSample = NULL;
			}
			if (pDbUsePort) {
				usMasterDbPort = (unsigned short)atoi(pDbUsePort);
				free(pDbUsePort);
				pDbUsePort = NULL;
			}
			if (pSlaveDbHost) {
				strcpy_s(szSlaveDbHost, sizeof(szSlaveDbHost), pSlaveDbHost);
				free(pSlaveDbHost);
				pSlaveDbHost = NULL;
			}
			if (pSlaveDbUser) {
				strcpy_s(szSlaveDbUser, sizeof(szSlaveDbUser), pSlaveDbUser);
				free(pSlaveDbUser);
				pSlaveDbUser = NULL;
			}
			if (pSlaveDbPasswd) {
				strcpy_s(szSlaveDbPasswd, sizeof(szSlaveDbPasswd), pSlaveDbPasswd);
				free(pSlaveDbPasswd);
				pSlaveDbPasswd = NULL;
			}
			if (pSlaveDbPort) {
				usSlaveDbPort = (unsigned short)atoi(pSlaveDbPort);
				free(pSlaveDbPort);
				pSlaveDbPort = NULL;
			}
			DbProxy * pInst = new DbProxy(szZkHost, g_szDllDir);
			if (pInst) {
				if (pInst->Start(szDbProxyHost, usDbProxyQryPort, szMidwareHost, usMidwarePublishPort, usMidwareTalkPort,
					usMidwareCollectPort, szMasterDbHost, szMasterDbUser, szMasterDbPasswd, usMasterDbPort, 
					strlen(szSlaveDbHost) == 0 ? szMasterDbHost : szSlaveDbHost, 
					strlen(szSlaveDbUser) == 0 ? szMasterDbUser : szSlaveDbUser, 
					strlen(szSlaveDbPasswd) == 0 ? szMasterDbPasswd: szSlaveDbPasswd, 
					usSlaveDbPort == 0 ? usMasterDbPort : usSlaveDbPort, 
					szDbMajorSample, szDbLocateSample) == 0) {
					unsigned long long ullInst = (unsigned long long)pInst;
					pthread_mutex_lock(&g_mutex4InstList);
					g_instList.insert(std::make_pair(ullInst, pInst));
					pthread_mutex_unlock(&g_mutex4InstList);
					result = ullInst;
				} else {
					delete pInst;
					pInst = NULL;
				}
			}
		}
	}
	return result;
}

int __stdcall DbProxy_Stop(unsigned long long ullInst_)
{
	int result = -1;
	pthread_mutex_lock(&g_mutex4InstList);
	if (!g_instList.empty()) {
		std::map<unsigned long long, DbProxy *>::iterator iter = g_instList.find(ullInst_);
		if (iter != g_instList.end()) {
			DbProxy * pInst = iter->second;
			if (pInst) {
				result = pInst->Stop();
				Sleep(10);
				delete pInst;
				pInst = NULL;
			}
			g_instList.erase(iter);
		}
	}
	pthread_mutex_unlock(&g_mutex4InstList);
	return result;
}

int __stdcall DbProxy_SetLogType(unsigned long long ullInst_, unsigned short usLogType_)
{
	int result = -1;
	pthread_mutex_lock(&g_mutex4InstList);
	if (!g_instList.empty()) {
		std::map<unsigned long long, DbProxy *>::iterator iter = g_instList.find(ullInst_);
		if (iter != g_instList.end()) {
			DbProxy * pInst = iter->second;
			if (pInst) {
				pInst->SetLogType(usLogType_);
				result = 0;
			}
		}
	}
	pthread_mutex_unlock(&g_mutex4InstList);
	return result;
}

