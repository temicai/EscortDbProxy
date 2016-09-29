#include "DbProxyConcrete.h"

zhash_t * DbProxy::g_deviceList = NULL;
zhash_t * DbProxy::g_guarderList = NULL;
zhash_t * DbProxy::g_taskList = NULL;
pthread_mutex_t DbProxy::g_mutex4DevList;
pthread_mutex_t DbProxy::g_mutex4GuarderList;
pthread_mutex_t DbProxy::g_mutex4TaskList;
unsigned int DbProxy::g_uiRefCount = 0;
unsigned short DbProxy::g_usSequence = 0;

DbProxy::DbProxy()
{
	m_nRun = 0;
	m_ctx = NULL;
	m_reception = NULL;
	m_subscriber = NULL;
	m_pthdNetwork.p = NULL;

	m_zkHandle = NULL;
	m_szZkHost[0] = '\0';
	m_zkNodePath[0] = '\0';
	m_bZKConnected = false;
	memset(&m_zkDbProxy, 0, sizeof(m_zkDbProxy));

	m_writeConn = NULL;
	m_readConn = NULL;
	pthread_mutex_init(&m_mutex4QryList, NULL);
	pthread_mutex_init(&m_mutex4ExecList, NULL);
	pthread_cond_init(&m_cond4QryList, NULL);
	pthread_cond_init(&m_cond4ExecList, NULL);
	m_pthdQuery.p = NULL;
	m_pthdExec.p = NULL;

	if (g_uiRefCount == 0) {
		g_deviceList = zhash_new();
		g_guarderList = zhash_new();
		g_taskList = zhash_new();
		pthread_mutex_init(&g_mutex4DevList, NULL);
		pthread_mutex_init(&g_mutex4GuarderList, NULL);
		pthread_mutex_init(&g_mutex4TaskList, NULL);
	}
	g_uiRefCount++;

	m_nLogInst = 0;
	m_nLogType = LOGTYPE_FILE;
	m_szLogRoot[0] = '\0';
	m_pthdLog.p = NULL;
	pthread_mutex_init(&m_mutex4LogQue, NULL);
	pthread_cond_init(&m_cond4LogQue, NULL);
	 



}

DbProxy::~DbProxy()
{

}