#include "DbProxyConcrete.h"

zhash_t * DbProxy::g_deviceList = NULL;
zhash_t * DbProxy::g_guarderList = NULL;
zhash_t * DbProxy::g_taskList = NULL;
zhash_t * DbProxy::g_personList = NULL;
zhash_t * DbProxy::g_orgList = NULL;
pthread_mutex_t DbProxy::g_mutex4DevList;
pthread_mutex_t DbProxy::g_mutex4GuarderList;
pthread_mutex_t DbProxy::g_mutex4TaskList;
pthread_mutex_t DbProxy::g_mutex4PersonList;
pthread_mutex_t DbProxy::g_mutex4OrgList;
pthread_mutex_t DbProxy::g_mutex4InteractSequence;
pthread_mutex_t DbProxy::g_mutex4PipeSequence;
pthread_mutex_t DbProxy::g_mutex4UpdateTime;
int DbProxy::g_nRefCount = 0;
unsigned int DbProxy::g_uiInteractSequence = 0;
unsigned short DbProxy::g_usPipeSequence = 0;
BOOL DbProxy::g_bLoadSql = FALSE;
//char DbProxy::g_szLastUpdateTime[20] = { 0 };
int DbProxy::g_nUpdatePipeState = 0;
unsigned long DbProxy::g_ulLastUpdateTime = 0;

static unsigned long strdatetime2time(const char * strDatetime)
{
	if (strDatetime) {
		struct tm tm_curr;
		sscanf_s(strDatetime, "%04d%02d%02d%02d%02d%02d", &tm_curr.tm_year, &tm_curr.tm_mon, 
			&tm_curr.tm_mday, &tm_curr.tm_hour, &tm_curr.tm_min, &tm_curr.tm_sec);
		tm_curr.tm_year -= 1900;
		tm_curr.tm_mon -= 1;
		return (unsigned long)mktime(&tm_curr);
	}
	return 0;
}

static unsigned long sqldatetime2time(const char * sqlDatetime)
{
	if (sqlDatetime) {
		struct tm tm_curr;
		sscanf_s(sqlDatetime, "%04d-%02d-%02d %02d:%02d:%02d", &tm_curr.tm_year, &tm_curr.tm_mon,
			&tm_curr.tm_mday, &tm_curr.tm_hour, &tm_curr.tm_min, &tm_curr.tm_sec);
		tm_curr.tm_year -= 1900;
		tm_curr.tm_mon -= 1;
		return (unsigned long)mktime(&tm_curr);
	}
	return 0;
}

static void format_datetime(unsigned long ulSrcTime, char * pStrDatetime, size_t nStrDatetimeLen)
{
	if (ulSrcTime > 0) {
		tm tm_time;
		time_t srcTime = ulSrcTime;
		localtime_s(&tm_time, &srcTime);
		char szDatetime[16] = { 0 };
		snprintf(szDatetime, sizeof(szDatetime), "%04d%02d%02d%02d%02d%02d", tm_time.tm_year + 1900,
			tm_time.tm_mon + 1, tm_time.tm_mday, tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec);
		size_t nLen = strlen(szDatetime);
		if (pStrDatetime && nStrDatetimeLen >= nLen) {
			strncpy_s(pStrDatetime, nStrDatetimeLen, szDatetime, nLen);
		}
	}
	else {
		if (pStrDatetime && nStrDatetimeLen) {
			pStrDatetime[0] = '\0';
		}
	}
}

static void format_sqldatetime(unsigned long ulSrcTime, char * pSqlDatetime, size_t nDatetimeLen)
{
	struct tm tm_time;
	time_t srcTime = ulSrcTime;
	localtime_s(&tm_time, &srcTime);
	char szDatetime[20] = { 0 };
	snprintf(szDatetime, sizeof(szDatetime), "%04d-%02d-%02d %02d:%02d:%02d", tm_time.tm_year + 1900,
		tm_time.tm_mon + 1, tm_time.tm_mday, tm_time.tm_hour, tm_time.tm_min, tm_time.tm_sec);
	size_t nLen = strlen(szDatetime);
	if (pSqlDatetime && nDatetimeLen >= nLen) {
		strncpy_s(pSqlDatetime, nDatetimeLen, szDatetime, strlen(szDatetime));
	}
}

static char * make_zkpath(int num, ...)
{
	const char * tmp_string;
	va_list arguments;
	va_start(arguments, num);
	int nTotalLen = 0;
	for (int i = 0; i < num; i++) {
		tmp_string = va_arg(arguments, const char *);
		if (tmp_string) {
			nTotalLen += strlen(tmp_string);
		}
	}
	va_end(arguments);
	char * path = (char *)malloc(nTotalLen + 1);
	if (path) {
		memset(path, 0, nTotalLen + 1);
		va_start(arguments, num);
		for (int i = 0; i < num; i++) {
			tmp_string = va_arg(arguments, const char *);
			if (tmp_string) {
				strcat_s(path, nTotalLen + 1, tmp_string);
			}
		}
	}
	return path;
}

DbProxy::DbProxy(const char * pZkHost_, const char * pRoot_)
{
	srand((unsigned int)time(NULL));
	m_nRun = 0;
	m_ctx = NULL;
	m_reception = NULL;
	m_subscriber = NULL;
	m_interactor = NULL;
	m_pthdNetwork.p = NULL;

	m_zkHandle = NULL;
	m_szZkHost[0] = '\0';
	m_zkNodePath[0] = '\0';
	m_bZKConnected = false;
	memset(&m_zkDbProxy, 0, sizeof(m_zkDbProxy));

	m_writeConn = NULL;
	m_readConn = NULL;
	pthread_mutex_init(&m_mutex4ReadConn, NULL);
	pthread_mutex_init(&m_mutex4QryQue, NULL);
	pthread_mutex_init(&m_mutex4ExecQue, NULL);
	pthread_mutex_init(&m_mutex4LocateQue, NULL);
	pthread_cond_init(&m_cond4QryQue, NULL);
	pthread_cond_init(&m_cond4ExecQue, NULL);
	pthread_cond_init(&m_cond4LocateQue, NULL);
	m_pthdQuery.p = NULL;
	m_pthdExec.p = NULL;
	m_pthdLocate.p = NULL;

	m_pthdTopicMsg.p = NULL;
	pthread_mutex_init(&m_mutex4TopicMsgQue, NULL);
	pthread_cond_init(&m_cond4TopicMsgQue, NULL);
	m_pthdInteractMsg.p = NULL;
	pthread_mutex_init(&m_mutex4InteractMsgQueu, NULL);
	pthread_cond_init(&m_cond4InteractMsgQue, NULL);

	memset(&m_remoteLink, 0, sizeof(m_remoteLink));
	pthread_mutex_init(&m_mutex4RemoteLink, NULL);

	pthread_mutex_init(&m_mutex4UpdatePipe, NULL);
	pthread_cond_init(&m_cond4UpdatePipe, NULL);
	m_pthdUpdatePipe.p = NULL;

	if (g_nRefCount == 0) {
		g_bLoadSql = FALSE;
		g_deviceList = zhash_new();
		g_guarderList = zhash_new();
		g_taskList = zhash_new();
		g_personList = zhash_new();
		g_orgList = zhash_new();
		g_ulLastUpdateTime = 0;
		g_nUpdatePipeState = dbproxy::E_PIPE_CLOSE; 
		pthread_mutex_init(&g_mutex4DevList, NULL);
		pthread_mutex_init(&g_mutex4GuarderList, NULL);
		pthread_mutex_init(&g_mutex4TaskList, NULL);
		pthread_mutex_init(&g_mutex4PersonList, NULL);
		pthread_mutex_init(&g_mutex4OrgList, NULL);
		pthread_mutex_init(&g_mutex4InteractSequence, NULL);
		pthread_mutex_init(&g_mutex4PipeSequence, NULL);
		pthread_mutex_init(&g_mutex4UpdateTime, NULL);
	}
	g_nRefCount++;

	m_nLogInst = 0;
	m_nLogType = LOGTYPE_FILE;
	m_szLogRoot[0] = '\0';
	m_pthdLog.p = NULL;
	pthread_mutex_init(&m_mutex4LogQue, NULL);
	pthread_cond_init(&m_cond4LogQue, NULL);
	 
	if (pZkHost_ && strlen(pZkHost_)) {
		strncpy_s(m_szZkHost, sizeof(m_szZkHost), pZkHost_, strlen(pZkHost_));	
	}
	initZookeeper();

	if (pRoot_ && strlen(pRoot_)) {
		strncpy_s(m_szLogRoot, sizeof(m_szLogRoot), pRoot_, strlen(pRoot_));
	}
	initLog();
	mysql_library_init(0, NULL, NULL);
}

DbProxy::~DbProxy()
{
	if (m_nRun) {
		Stop();
	}
	g_nRefCount--;
	if (g_nRefCount <= 0) {
		g_bLoadSql = FALSE;
		if (g_deviceList) {
			zhash_destroy(&g_deviceList);
		}
		if (g_guarderList) {
			zhash_destroy(&g_guarderList);
		}
		if (g_taskList) {
			zhash_destroy(&g_taskList);
		}
		if (g_personList) {
			zhash_destroy(&g_personList);
		}
		if (g_orgList) {
			zhash_destroy(&g_orgList);
		}
		pthread_mutex_destroy(&g_mutex4DevList);
		pthread_mutex_destroy(&g_mutex4GuarderList);
		pthread_mutex_destroy(&g_mutex4TaskList);
		pthread_mutex_destroy(&g_mutex4PersonList);
		pthread_mutex_destroy(&g_mutex4OrgList);
		pthread_mutex_destroy(&g_mutex4InteractSequence);
		pthread_mutex_destroy(&g_mutex4PipeSequence);
		pthread_mutex_destroy(&g_mutex4UpdateTime);
		g_nRefCount = 0;
	}
	pthread_mutex_destroy(&m_mutex4LogQue);
	pthread_cond_destroy(&m_cond4LogQue);
	pthread_mutex_destroy(&m_mutex4QryQue);
	pthread_cond_destroy(&m_cond4QryQue);
	pthread_mutex_destroy(&m_mutex4ExecQue);
	pthread_cond_destroy(&m_cond4ExecQue);
	pthread_mutex_destroy(&m_mutex4LocateQue);
	pthread_cond_destroy(&m_cond4LocateQue);
	pthread_mutex_destroy(&m_mutex4TopicMsgQue);
	pthread_cond_destroy(&m_cond4TopicMsgQue);
	pthread_mutex_destroy(&m_mutex4InteractMsgQueu);
	pthread_cond_destroy(&m_cond4InteractMsgQue);
	pthread_mutex_destroy(&m_mutex4RemoteLink);
	pthread_mutex_destroy(&m_mutex4ReadConn);
	pthread_mutex_destroy(&m_mutex4UpdatePipe);
	pthread_cond_destroy(&m_cond4UpdatePipe);

	if (m_ctx) {
		zctx_destroy(&m_ctx);
	}
	if (m_nLogInst) {
		LOG_Release(m_nLogInst);
		m_nLogInst = 0;
	}
	if (m_zkHandle) {
		zookeeper_close(m_zkHandle);
		m_zkHandle = NULL;
	}
	mysql_library_end();
}

int DbProxy::Start(const char * pHost_, unsigned short usReceptPort_, const char * pMidwareHost_, 
	unsigned short usPublishPort_, unsigned short usContactPort_, unsigned short usCollectPort_,
	const char * pDbHost_, const char * pDbUser_, const char * pDbPasswd_, const char * pDbName_, 
	const char * pDbAuxName_)
{
	if (m_nRun) {
		return 0;
	}
	memset(&m_remoteLink, 0, sizeof(m_remoteLink));
	if (m_ctx == NULL) {
		m_ctx = zctx_new();
	}
	m_interactor = zsocket_new(m_ctx, ZMQ_DEALER);
	zuuid_t * uuid = zuuid_new();
	const char * szUuid = zuuid_str(uuid);
	zsocket_set_identity(m_interactor, szUuid);
	zsocket_connect(m_interactor, "tcp://%s:%u", strlen(pMidwareHost_) ? pMidwareHost_ : "127.0.0.1",
		usContactPort_ > 0 ? usContactPort_ : 25001);
	zuuid_destroy(&uuid);

	m_subscriber = zsocket_new(m_ctx, ZMQ_SUB);
	zsocket_set_subscribe(m_subscriber, "");
	zsocket_connect(m_subscriber, "tcp://%s:%u", strlen(pMidwareHost_) ? pMidwareHost_ : "127.0.0.1",
		usPublishPort_ > 0 ? usPublishPort_ : 25000);

	m_pipeline = zsocket_new(m_ctx, ZMQ_DEALER);
	uuid = zuuid_new();
	const char * szUuid2 = zuuid_str(uuid);
	zsocket_set_identity(m_pipeline, szUuid2);
	zsocket_connect(m_pipeline, "tcp://%s:%u", strlen(pMidwareHost_) ? pMidwareHost_ : "127.0.0.1",
		usCollectPort_ > 0 ? usCollectPort_ : 25002);

	m_reception = zsocket_new(m_ctx, ZMQ_ROUTER);
	zsocket_bind(m_reception, "tcp://*:%u", usReceptPort_);

	m_readConn = mysql_init(NULL);
	m_writeConn = mysql_init(NULL);
	m_locateConn = mysql_init(NULL);
	m_updateConn = mysql_init(NULL);
	if (m_readConn && m_writeConn && m_locateConn && m_updateConn) {
		if (mysql_real_connect(m_readConn, (pDbHost_ && strlen(pDbHost_)) ? pDbHost_ : "127.0.0.1", 
			pDbUser_, pDbPasswd_, (pDbName_ && strlen(pDbName_)) ? pDbName_ : DBNAME_MAJOR, 3306, NULL, 0) 
			&& mysql_real_connect(m_writeConn, (pDbHost_ && strlen(pDbHost_)) ? pDbHost_ : "127.0.0.1",
			pDbUser_, pDbPasswd_, (pDbName_ && strlen(pDbName_)) ? pDbName_ : DBNAME_MAJOR, 3306, NULL, 0)
			&& mysql_real_connect(m_locateConn, (pDbHost_ && strlen(pDbHost_)) ? pDbHost_ : "127.0.0.1", 
			pDbUser_, pDbPasswd_, (pDbAuxName_ && strlen(pDbAuxName_)) ? pDbAuxName_ : DBNAME_LOCATE, 3306, 
			NULL, 0) && mysql_real_connect(m_updateConn, (pDbHost_ && strlen(pDbHost_)) ? pDbHost_ 
			: "127.0.0.1", pDbUser_, pDbPasswd_, (pDbName_ && strlen(pDbName_)) ? pDbName_ : DBNAME_MAJOR,
			3306, NULL, 0)) {
			mysql_set_character_set(m_readConn, "gb2312");
			mysql_set_character_set(m_writeConn, "gb2312");
			mysql_set_character_set(m_locateConn, "gb2312");
			mysql_set_character_set(m_updateConn, "gb2312");

			m_nRun = 1;
			m_nTimerTickCount = 0;
			m_loop = zloop_new();
			m_nTimer4Supervise = zloop_timer(m_loop, 10000, 0, supervise, this);

			if (m_pthdLog.p == NULL) {
				pthread_create(&m_pthdLog, NULL, dealLogThread, this);
			}
			if (m_pthdNetwork.p == NULL) {
				pthread_create(&m_pthdNetwork, NULL, dealNetworkThread, this);
			}
			if (m_pthdQuery.p == NULL) {
				pthread_create(&m_pthdQuery, NULL, dealSqlQueryThread, this);
			}
			if (m_pthdExec.p == NULL) {
				pthread_create(&m_pthdExec, NULL, dealSqlExecThread, this);
			}
			if (m_pthdLocate.p == NULL) {
				pthread_create(&m_pthdLocate, NULL, dealSqlLocateThread, this);
			}
			if (m_pthdTopicMsg.p == NULL) {
				pthread_create(&m_pthdTopicMsg, NULL, dealTopicMsgThread, this);
			}
			if (m_pthdInteractMsg.p == NULL) {
				pthread_create(&m_pthdInteractMsg, NULL, dealInteractMsgThread, this);
			}
			if (m_pthdUpdatePipe.p == NULL) { //start Update pipe 
				pthread_create(&m_pthdUpdatePipe, NULL, dealUpdatePipeThread, this);
			}
			if (m_pthdSupervise.p == NULL) {
				pthread_create(&m_pthdSupervise, NULL, superviseThread, this);
			}

			if (pHost_ && strlen(pHost_)) {
				strncpy_s(m_zkDbProxy.szProxyHostIp, sizeof(m_zkDbProxy.szProxyHostIp), pHost_, 
					strlen(pHost_));
			}
			else {
				snprintf(m_zkDbProxy.szProxyHostIp, sizeof(m_zkDbProxy.szProxyHostIp), "127.0.0.1");
			}
			if (pDbHost_ && strlen(pDbHost_)) {
				strncpy_s(m_zkDbProxy.szDbHostIp, sizeof(m_zkDbProxy.szDbHostIp), pDbHost_, 
					strlen(pDbHost_));
			}
			if (pDbUser_ && strlen(pDbUser_)) {
				strncpy_s(m_zkDbProxy.szDbUser, sizeof(m_zkDbProxy.szDbUser), pDbUser_, 
					strlen(pDbUser_));
			}
			if (pDbPasswd_ && strlen(pDbPasswd_)) {
				strncpy_s(m_zkDbProxy.szDbPasswd, sizeof(m_zkDbProxy.szDbPasswd), pDbPasswd_, 
					strlen(pDbPasswd_));
			}
			if (pDbName_ && strlen(pDbName_)) {
				strncpy_s(m_zkDbProxy.szMajorSample, sizeof(m_zkDbProxy.szMajorSample), pDbName_,
					strlen(pDbName_));
			}
			if (pDbAuxName_ && strlen(pDbAuxName_)) {
				strncpy_s(m_zkDbProxy.szLocateSample, sizeof(m_zkDbProxy.szLocateSample),
					pDbAuxName_, strlen(pDbAuxName_));
			}

			competeForMaster();

			if (g_bLoadSql == FALSE) {
				if (initSqlBuffer()) {
					unsigned long ulTime = (unsigned long)time(NULL);
					g_ulLastUpdateTime = ulTime;
					g_bLoadSql = TRUE;
				}
			}

			char szLog[256] = { 0 };
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]Database proxy start %u\r\n", __FUNCTION__, 
				__LINE__, usReceptPort_);
			writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);

			return 0;
		}
	}
	zsocket_destroy(m_ctx, m_subscriber);
	zsocket_destroy(m_ctx, m_reception);
	zsocket_destroy(m_ctx, m_interactor);
	zctx_destroy(&m_ctx);
	if (m_readConn) {
		mysql_close(m_readConn);
		m_readConn = NULL;
	}
	if (m_writeConn) {
		mysql_close(m_writeConn);
		m_writeConn = NULL;
	}
	if (m_locateConn) {
		mysql_close(m_locateConn);
		m_locateConn = NULL;
	}
	if (m_updateConn) {
		mysql_close(m_updateConn);
		m_updateConn = NULL;
	}
	return -1;
}

int DbProxy::Stop()
{
	if (!m_nRun) {
		return 0;
	}
	m_nRun = 0;
	char szLog[256] = { 0 };
	snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]Database proxy stop\r\n", __FUNCTION__, __LINE__);
	writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
	if (m_pthdSupervise.p) {
		pthread_join(m_pthdSupervise, NULL);
		m_pthdSupervise.p = NULL;
	}
	if (m_pthdNetwork.p) {
		pthread_join(m_pthdNetwork, NULL);
		m_pthdNetwork.p = NULL;
	}
	if (m_ctx) {
		zsocket_destroy(m_ctx, m_subscriber);
		zsocket_destroy(m_ctx, m_reception);
		zsocket_destroy(m_ctx, m_interactor);
		zsocket_destroy(m_ctx, m_pipeline);
		m_subscriber = NULL;
		m_reception = NULL;
		m_interactor = NULL;
		m_pipeline = NULL;
		zctx_destroy(&m_ctx);
	}
	if (m_pthdQuery.p) {
		pthread_cond_broadcast(&m_cond4QryQue);
		pthread_join(m_pthdQuery, NULL);
		m_pthdQuery.p = NULL;
	}
	if (m_pthdExec.p) {
		pthread_cond_broadcast(&m_cond4ExecQue);
		pthread_join(m_pthdExec, NULL);
		m_pthdExec.p = NULL;
	}
	if (m_pthdLocate.p) {
		pthread_cond_broadcast(&m_cond4LocateQue);
		pthread_join(m_pthdLocate, NULL);
		m_pthdLocate.p = NULL;
	}
	if (m_pthdTopicMsg.p) {
		pthread_cond_broadcast(&m_cond4TopicMsgQue);
		pthread_join(m_pthdTopicMsg, NULL);
		m_pthdTopicMsg.p = NULL;
	}
	if (m_pthdInteractMsg.p) {
		pthread_cond_broadcast(&m_cond4InteractMsgQue);
		pthread_join(m_pthdInteractMsg, NULL);
		m_pthdInteractMsg.p = NULL;
	}
	if (m_pthdUpdatePipe.p) {
		pthread_cond_broadcast(&m_cond4UpdatePipe);
		pthread_join(m_pthdUpdatePipe, NULL);
		m_pthdUpdatePipe.p = NULL;
	}
	if (m_pthdLog.p) {
		pthread_cond_broadcast(&m_cond4LogQue);
		pthread_join(m_pthdLog, NULL);
		m_pthdLog.p = NULL;
	}
	if (m_loop) {
		zloop_timer_end(m_loop, m_nTimer4Supervise);
		zloop_destroy(&m_loop);
	}
	if (m_bZKConnected) {	
		zoo_delete(m_zkHandle, m_zkNodePath, -1);
		m_zkNodePath[0] = '\0';
		m_bZKConnected = false;
	}
	if (m_readConn) {
		mysql_close(m_readConn);
		m_readConn = NULL;
	}
	if (m_writeConn) {
		mysql_close(m_writeConn);
		m_writeConn = NULL;
	}
	if (m_locateConn) {
		mysql_close(m_locateConn);
		m_locateConn = NULL;
	}
	if (m_updateConn) {
		mysql_close(m_updateConn);
		m_updateConn;
	}
	return 0;
}

int DbProxy::GetState()
{
	return m_nRun;
}

void DbProxy::SetLogType(int nLogType_)
{
	if (m_nLogInst) {
		if (m_nLogType != nLogType_) {
			LOG_INFO logInfo;
			LOG_GetConfig(m_nLogInst, &logInfo);
			if (logInfo.type != nLogType_) {
				logInfo.type = nLogType_;
				LOG_SetConfig(m_nLogInst, logInfo);
			}
			m_nLogType = logInfo.type;
		}
	}
}

void DbProxy::initLog()
{
	if (m_nLogInst == 0) {
		m_nLogInst = LOG_Init();
		if (m_nLogInst) {
			LOG_INFO logInfo;
			logInfo.type = m_nLogType;
			char szLogDir[256] = { 0 };
			snprintf(szLogDir, 256, "%slog\\", m_szLogRoot);
			CreateDirectoryExA(".\\", szLogDir, NULL);
			strcat_s(szLogDir, 256, "escort_dbproxy\\");
			CreateDirectoryExA(".\\", szLogDir, NULL);
			strncpy_s(logInfo.path, sizeof(logInfo.path), szLogDir, strlen(szLogDir));
			LOG_SetConfig(m_nLogInst, logInfo);
		}
	}
}

bool DbProxy::addLog(dbproxy::LogContext * pLogCtx_)
{
	bool result = false;
	if (pLogCtx_ && pLogCtx_->pLogData && pLogCtx_->uiDataLen) {
		pthread_mutex_lock(&m_mutex4LogQue);
		m_logQue.push(pLogCtx_);
		if (m_logQue.size() == 1) {
			pthread_cond_signal(&m_cond4LogQue);
		}
		result = true;
		pthread_mutex_unlock(&m_mutex4LogQue);
	}
	return result;
}

void DbProxy::writeLog(const char * pLogContent_, int nLogCategoryType_, int nLogType_)
{
	if (pLogContent_ && strlen(pLogContent_)) {
		dbproxy::LogContext * pLog = new dbproxy::LogContext();
		pLog->nLogCategory = nLogCategoryType_;
		pLog->nLogType = nLogType_;
		size_t nSize = strlen(pLogContent_);
		pLog->uiDataLen = (unsigned int)nSize;
		pLog->pLogData = (char *)malloc(nSize + 1);
		if (pLog->pLogData) {
			memcpy_s(pLog->pLogData, nSize, pLogContent_, nSize);
			pLog->pLogData[nSize] = '\0';
		}
		if (!addLog(pLog)) {
			free(pLog->pLogData);
			pLog->pLogData = NULL;
			delete pLog;
			pLog = NULL;
		}
	}
}

void DbProxy::dealLog()
{
	do {
		pthread_mutex_lock(&m_mutex4LogQue);
		while (m_nRun && m_logQue.empty()) {
			pthread_cond_wait(&m_cond4LogQue, &m_mutex4LogQue);
		}
		if (!m_nRun && m_logQue.empty()) {
			pthread_mutex_unlock(&m_mutex4LogQue);
			break;
		}
		dbproxy::LogContext * pLog = m_logQue.front();
		m_logQue.pop();
		pthread_mutex_unlock(&m_mutex4LogQue);
		if (pLog) {
			if (pLog->pLogData) {
				if (m_nLogInst) {
					LOG_Log(m_nLogInst, pLog->pLogData, pLog->nLogCategory, pLog->nLogType);
				}
				free(pLog->pLogData);
				pLog->pLogData = NULL;
			}
			delete pLog;
			pLog = NULL;
		}
	} while (1);
}

bool DbProxy::addSqlTransaction(dbproxy::SqlTransaction * pTransaction_, int nTransactionType_)
{
	bool result = false;
	if (pTransaction_ && pTransaction_->pSqlList && pTransaction_->uiSqlCount) {
		if (nTransactionType_ == SQLTYPE_EXECUTE) {
			pthread_mutex_lock(&m_mutex4ExecQue);
			m_sqlExecuteQue.push(pTransaction_);
			if (m_sqlExecuteQue.size() == 1) {
				pthread_cond_signal(&m_cond4ExecQue);
			}
			result = true;
			pthread_mutex_unlock(&m_mutex4ExecQue);
		}
		else if (nTransactionType_ == SQLTYPE_QUERY) {
			pthread_mutex_lock(&m_mutex4QryQue);
			m_sqlQueryQue.push(pTransaction_);
			if (m_sqlQueryQue.size() == 1) {
				pthread_cond_signal(&m_cond4QryQue);
			}
			result = true;
			pthread_mutex_unlock(&m_mutex4QryQue);
		}
		else {
			pthread_mutex_lock(&m_mutex4LocateQue);
			m_sqlLocateQue.push(pTransaction_);
			if (m_sqlLocateQue.size() == 1) {
				pthread_cond_signal(&m_cond4LocateQue);
			}
			result = true;
			pthread_mutex_unlock(&m_mutex4LocateQue);
		}
	}
	return result;
}

void DbProxy::dealSqlQuery()
{
	do {
		pthread_mutex_lock(&m_mutex4QryQue);
		while (m_nRun && m_sqlQueryQue.empty()) {
			pthread_cond_wait(&m_cond4QryQue, &m_mutex4QryQue);
		}
		if (!m_nRun && m_sqlQueryQue.empty()) {
			pthread_mutex_unlock(&m_mutex4QryQue);
			break;
		}
		dbproxy::SqlTransaction * pTransaction = m_sqlQueryQue.front();
		m_sqlQueryQue.pop();
		pthread_mutex_unlock(&m_mutex4QryQue);
		if (pTransaction) {
			if (pTransaction->pSqlList && pTransaction->uiSqlCount) {
				for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
					if (pTransaction->pSqlList[i].pStatement && pTransaction->pSqlList[i].ulStatementLen) {
						handleSqlQry(&pTransaction->pSqlList[i], pTransaction->uiTransactionSequence, 
							pTransaction->ulTransactionTime, pTransaction->szTransactionFrom);
						free(pTransaction->pSqlList[i].pStatement);
					}
				}
				free(pTransaction->pSqlList);
				pTransaction->pSqlList = NULL;
				pTransaction->uiSqlCount = 0;
			}
			free(pTransaction);
			pTransaction = NULL;
		}
	} while (1);
}

void DbProxy::dealSqlExec()
{
	do {
		pthread_mutex_lock(&m_mutex4ExecQue);
		while (m_nRun && m_sqlExecuteQue.empty()) {
			pthread_cond_wait(&m_cond4ExecQue, &m_mutex4ExecQue);
		}
		if (!m_nRun && m_sqlExecuteQue.empty()) {
			pthread_mutex_unlock(&m_mutex4ExecQue);
			break;
		}
		dbproxy::SqlTransaction * pTransaction = m_sqlExecuteQue.front();
		m_sqlExecuteQue.pop();
		pthread_mutex_unlock(&m_mutex4ExecQue);
		if (pTransaction) {
			if (pTransaction->pSqlList && pTransaction->uiSqlCount) {
				for (unsigned int i = 0; i < pTransaction->uiSqlCount; ++i) {
					if (pTransaction->pSqlList[i].pStatement && pTransaction->pSqlList[i].ulStatementLen) {
						handleSqlExec(&pTransaction->pSqlList[i], pTransaction->uiTransactionSequence, 
							pTransaction->ulTransactionTime, pTransaction->szTransactionFrom);
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
					}
				}
				free(pTransaction->pSqlList);
				pTransaction->pSqlList = NULL;
				pTransaction->uiSqlCount = 0;
			}
			free(pTransaction);
			pTransaction = NULL;
		}
	} while (1);
}

void DbProxy::dealSqlLocate()
{
	do {
		pthread_mutex_lock(&m_mutex4LocateQue);
		while (m_nRun && m_sqlLocateQue.empty()) {
			pthread_cond_wait(&m_cond4LocateQue, &m_mutex4LocateQue);
		}
		if (!m_nRun && m_sqlLocateQue.empty()) {
			pthread_mutex_unlock(&m_mutex4LocateQue);
			break;
		}
		dbproxy::SqlTransaction * pTransaction = m_sqlLocateQue.front();
		m_sqlLocateQue.pop();
		pthread_mutex_unlock(&m_mutex4LocateQue);
		if (pTransaction) {
			if (pTransaction->pSqlList && pTransaction->uiSqlCount) {
				for (unsigned int i = 0; i < pTransaction->uiSqlCount; ++i) {
					if (pTransaction->pSqlList[i].pStatement && pTransaction->pSqlList[i].ulStatementLen) {
						handleSqlLocate(&pTransaction->pSqlList[i], pTransaction->uiTransactionSequence, 
							pTransaction->ulTransactionTime);
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
						pTransaction->pSqlList[i].ulStatementLen = 0;
					}
				}
				free(pTransaction->pSqlList);
				pTransaction->pSqlList = NULL;
				pTransaction->uiSqlCount = 0;
			}
			free(pTransaction);
			pTransaction = NULL;
		}
	} while (1);
}

void DbProxy::handleSqlQry(const dbproxy::SqlStatement * pSqlStatement_, unsigned int uiQrySeq_, 
	unsigned long ulQryTime_, const char * pQryFrom_)
{
	char szLog[512] = { 0 };
	pthread_mutex_lock(&m_mutex4ReadConn);
	if (m_readConn && pSqlStatement_ && pSqlStatement_->pStatement && pSqlStatement_->ulStatementLen) {
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]seq=%u, time=%lu, execute query sql: %s\r\n",
			__FUNCTION__, __LINE__, uiQrySeq_, ulQryTime_, pSqlStatement_->pStatement);
		//writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
		printf(szLog);
		int nErr = mysql_real_query(m_readConn, pSqlStatement_->pStatement, pSqlStatement_->ulStatementLen);
		if (nErr == 0) {
			MYSQL_RES * res_ptr = mysql_store_result(m_readConn);
			if (res_ptr) {
				my_ulonglong nRowCount = mysql_num_rows(res_ptr);
				if (nRowCount > 0) {
					MYSQL_ROW row;
					size_t nCount = (size_t)nRowCount;
					if (pSqlStatement_->uiCorrelativeTable == escort_db::E_TBL_DEVICE) {					
						escort_db::SqlDevice * pSqlDevList = (escort_db::SqlDevice *)zmalloc(nCount 
							* sizeof(escort_db::SqlDevice));
						size_t nDeviceSize = sizeof(WristletDevice);
						WristletDevice * pDevList = (WristletDevice *)zmalloc(nCount * nDeviceSize);
						int i = 0;
						while (row = mysql_fetch_row(res_ptr)) {
							strncpy_s(pSqlDevList[i].szDeviceId, sizeof(pSqlDevList[i].szDeviceId), row[0], strlen(row[0]));
							strncpy_s(pSqlDevList[i].szFactoryId, sizeof(pSqlDevList[i].szFactoryId), row[1], strlen(row[1]));
							strncpy_s(pSqlDevList[i].szOrgId, sizeof(pSqlDevList[i].szOrgId), row[2], strlen(row[2]));
							if (row[3] && strlen(row[3])) {
								strncpy_s(pSqlDevList[i].szLastCommuncation, sizeof(pSqlDevList[i].szLastCommuncation),
									row[3], strlen(row[3]));
							}
							if (row[4] && strlen(row[4])) {
								strncpy_s(pSqlDevList[i].szLastLocation, sizeof(pSqlDevList[i].szLastLocation), row[4],
									strlen(row[4]));
							}
							if (row[5]) {
								pSqlDevList[i].dLat = atof(row[5]);
							}
							if (row[6]) {
								pSqlDevList[i].dLng = atof(row[6]);
							}
							if (row[7]) {
								pSqlDevList[i].nLocationType = atoi(row[7]);
							}
							if (row[8]) {
								pSqlDevList[i].usIsUse = atoi(row[8]);
							}
							if (row[9]) {
								pSqlDevList[i].usBattery = (unsigned short)atoi(row[9]);
							}
							if (row[10]) {
								pSqlDevList[i].usOnline = (unsigned short)atoi(row[10]);
							}
							if (row[11]) {
								pSqlDevList[i].usIsRemove = (unsigned short)atoi(row[11]);
							}
							WristletDevice * pDev = (WristletDevice *)zmalloc(nDeviceSize);
							memset(pDev, 0, nDeviceSize);
							pDev->deviceBasic.nBattery = pSqlDevList[i].usBattery;
							strncpy_s(pDev->deviceBasic.szDeviceId, sizeof(pDev->deviceBasic.szDeviceId),
								pSqlDevList[i].szDeviceId, strlen(pSqlDevList[i].szDeviceId));
							strncpy_s(pDev->deviceBasic.szFactoryId, sizeof(pDev->deviceBasic.szFactoryId),
								pSqlDevList[i].szFactoryId, strlen(pSqlDevList[i].szFactoryId));
							strncpy_s(pDev->szOrganization, sizeof(pDev->szOrganization), pSqlDevList[i].szOrgId,
								strlen(pSqlDevList[i].szOrgId));
							if (pSqlDevList[i].nLocationType == escort_db::E_LOCATE_APP) {
								pDev->guardPosition.dLatitude = pSqlDevList[i].dLat;
								pDev->guardPosition.dLngitude = pSqlDevList[i].dLng;
								pDev->nLastLocateType = LOCATE_BT;
							}
							else {
								pDev->devicePosition.dLatitude = pSqlDevList[i].dLat;
								pDev->devicePosition.dLngitude = pSqlDevList[i].dLng;
								pDev->nLastLocateType = (pSqlDevList[i].nLocationType == escort_db::E_LOCATE_GPS) ?
									LOCATE_GPS : LOCATE_LBS;
							}
							if (strlen(pSqlDevList[i].szLastLocation)) {
								pDev->ulLastLocateTime = sqldatetime2time(pSqlDevList[i].szLastLocation);
							}
							if (strlen(pSqlDevList[i].szLastCommuncation)) {
								pDev->deviceBasic.ulLastActiveTime = sqldatetime2time(pSqlDevList[i].szLastCommuncation);
							}
							if (pSqlDevList[i].usOnline) {
								changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
								if (pSqlDevList[i].usIsRemove) {
									changeDeviceStatus(DEV_LOOSE, pDev->deviceBasic.nStatus);
								}
								if (pSqlDevList[i].usBattery < 20) {
									changeDeviceStatus(DEV_LOWPOWER, pDev->deviceBasic.nStatus);
								}
							}
							else {
								pDev->deviceBasic.nStatus = DEV_OFFLINE;
							}						
							memcpy_s(&pDevList[i], nDeviceSize, pDev, nDeviceSize);
							pthread_mutex_lock(&g_mutex4DevList);
							zhash_update(g_deviceList, pSqlDevList[i].szDeviceId, pDev);
							zhash_freefn(g_deviceList, pSqlDevList[i].szDeviceId, free);
							pthread_mutex_unlock(&g_mutex4DevList);				

							i++;
						}
						if (pQryFrom_) {
							replyQuery(pDevList, nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_,
								pQryFrom_);
						}
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query row=%u, seq=%u, time=%lu, from=%s, "
							"sql=%s\r\n", __FUNCTION__, __LINE__, nCount, uiQrySeq_, ulQryTime_,
							pQryFrom_ ? pQryFrom_ : "", pSqlStatement_->pStatement);
						writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
						if (pSqlDevList) {
							free(pSqlDevList);
							pSqlDevList = NULL;
						}
						if (pDevList) {
							free(pDevList);
							pDevList = NULL;
						}
					}
					else if (pSqlStatement_->uiCorrelativeTable == escort_db::E_TBL_GUARDER) {
						escort_db::SqlGuarder * pSqlGuarderList = (escort_db::SqlGuarder *)zmalloc(nCount
							* sizeof(escort_db::SqlGuarder));
						size_t nGuarderSize = sizeof(Guarder);
						Guarder * pGuarderList = (Guarder *)zmalloc(nCount * nGuarderSize);
						int i = 0;
						while (row = mysql_fetch_row(res_ptr)) {
							strncpy_s(pSqlGuarderList[i].szUserId, sizeof(pSqlGuarderList[i].szUserId),
								row[0], strlen(row[0]));
							if (row[1] && strlen(row[1])) {
								strncpy_s(pSqlGuarderList[i].szUserName, sizeof(pSqlGuarderList[i].szUserName),
									row[1], strlen(row[1]));
							}
							if (row[2] && strlen(row[2])) {
								strncpy_s(pSqlGuarderList[i].szPasswd, sizeof(pSqlGuarderList[i].szPasswd),
									row[2], strlen(row[2]));
							}
							if (row[3] && strlen(row[3])) {
								strncpy_s(pSqlGuarderList[i].szOrgId, sizeof(pSqlGuarderList[i].szOrgId),
									row[3], strlen(row[3]));
							}

							Guarder * pGuarder = (Guarder *)zmalloc(nGuarderSize);
							memset(pGuarder, 0, nGuarderSize);
							strncpy_s(pGuarder->szId, sizeof(pGuarder->szId), pSqlGuarderList[i].szUserId,
								strlen(pSqlGuarderList[i].szUserId));
							strncpy_s(pGuarder->szTagName, sizeof(pGuarder->szTagName), pSqlGuarderList[i].szUserName,
								strlen(pSqlGuarderList[i].szUserName));
							strncpy_s(pGuarder->szOrg, sizeof(pGuarder->szOrg), pSqlGuarderList[i].szOrgId,
								strlen(pSqlGuarderList[i].szOrgId));
							strncpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword), pSqlGuarderList[i].szPasswd,
								strlen(pSqlGuarderList[i].szPasswd));
							pGuarder->uiState = STATE_GUARDER_FREE;

							memcpy_s(&pGuarderList[i], nGuarderSize, pGuarder, nGuarderSize);

							pthread_mutex_lock(&g_mutex4GuarderList);
							zhash_update(g_guarderList, pSqlGuarderList[i].szUserId, pGuarder);
							zhash_freefn(g_guarderList, pSqlGuarderList[i].szUserId, free);
							pthread_mutex_unlock(&g_mutex4GuarderList);

							i++;
						}
						if (pQryFrom_) {
							replyQuery(pGuarderList, nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_, 
								pQryFrom_);
						}
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query row=%u, seq=%u, time=%lu, from=%s, "
							"sql=%s\r\n", __FUNCTION__, __LINE__, nCount, uiQrySeq_, ulQryTime_,
							pQryFrom_ ? pQryFrom_ : "", pSqlStatement_->pStatement);
						writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
						if (pSqlGuarderList) {
							free(pSqlGuarderList);
							pSqlGuarderList = NULL;
						}
						if (pGuarderList) {
							free(pGuarderList);
							pGuarderList = NULL;
						}
					}
					else if (pSqlStatement_->uiCorrelativeTable == escort_db::E_TBL_TASK) {
						escort_db::SqlTask * pSqlTaskList = (escort_db::SqlTask *)zmalloc(nCount
							* sizeof(escort_db::SqlTask));
						size_t nTaskSize = sizeof(EscortTask);
						EscortTask * pTaskList = (EscortTask *)zmalloc(nCount * nTaskSize);
						int i = 0;
						while (row = mysql_fetch_row(res_ptr)) {
							strncpy_s(pSqlTaskList[i].szTaskId, sizeof(pSqlTaskList[i].szTaskId), row[0], 
								strlen(row[0]));
							pSqlTaskList[i].usTaskType = (unsigned short)atoi(row[1]);
							pSqlTaskList[i].usTaskLimit = (unsigned short)atoi(row[2]);
							if (row[3] && strlen(row[3])) {
								strncpy_s(pSqlTaskList[i].szStartTime, sizeof(pSqlTaskList[i].szStartTime),
									row[3], strlen(row[3]));
							}
							if (row[4] && strlen(row[4])) {
								strncpy_s(pSqlTaskList[i].szDestination, sizeof(pSqlTaskList[i].szDestination),
									row[4], strlen(row[4]));
							}
							if (row[5] && strlen(row[5])) {
								strncpy_s(pSqlTaskList[i].szGuarderId, sizeof(pSqlTaskList[i].szGuarderId),
									row[5], strlen(row[5]));
							}
							if (row[6] && strlen(row[6])) {
								strncpy_s(pSqlTaskList[i].szDeviceId, sizeof(pSqlTaskList[i].szDeviceId),
									row[6], strlen(row[6]));
							}
							if (row[7] && strlen(row[7])) {
								strncpy_s(pSqlTaskList[i].person.szPersonId, sizeof(pSqlTaskList[i].person.szPersonId),
									row[7], strlen(row[7]));
							}
							if (row[8] && strlen(row[8])) {
								strncpy_s(pSqlTaskList[i].person.szPersonName, sizeof(pSqlTaskList[i].person.szPersonName),
									row[8], strlen(row[8]));
							}
							if (row[9]) {
								pSqlTaskList[i].nFleeFlag = atoi(row[9]);
							}
							EscortTask * pTask = (EscortTask *)zmalloc(nTaskSize);
							memset(pTask, 0, sizeof(EscortTask));
							strncpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), pSqlTaskList[i].szTaskId,
								strlen(pSqlTaskList[i].szTaskId));
							strncpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), pSqlTaskList[i].szDeviceId,
								strlen(pSqlTaskList[i].szDeviceId));
							strncpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), pSqlTaskList[i].szGuarderId,
								strlen(pSqlTaskList[i].szGuarderId));
							unsigned long ulTaskStartTime = sqldatetime2time(pSqlTaskList[i].szStartTime);						
							format_datetime(ulTaskStartTime, pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime));					
							strncpy_s(pTask->szDestination, sizeof(pTask->szDestination), pSqlTaskList[i].szDestination,
								strlen(pSqlTaskList[i].szDestination));
							pTask->nTaskType = (uint8_t)pSqlTaskList[i].usTaskType;
							pTask->nTaskLimitDistance = (uint8_t)pSqlTaskList[i].usTaskLimit;
							snprintf(pTask->szTarget, sizeof(pTask->szTarget), "%s&%s", pSqlTaskList[i].person.szPersonId,
								pSqlTaskList[i].person.szPersonName);
							pTask->nTaskFlee = pSqlTaskList[i].nFleeFlag;

							pthread_mutex_lock(&g_mutex4DevList);
							WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pSqlTaskList[i].szDeviceId);
							if (pDev) {
								strncpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), pDev->deviceBasic.szFactoryId,
									strlen(pDev->deviceBasic.szFactoryId));
								if (pSqlTaskList[i].nFleeFlag) {
									changeDeviceStatus(DEV_FLEE, pDev->deviceBasic.nStatus);
								}
								else {
									changeDeviceStatus(DEV_GUARD, pDev->deviceBasic.nStatus);
								}
								strncpy_s(pDev->szBindGuard, sizeof(pDev->szBindGuard), pSqlTaskList[i].szGuarderId,
									strlen(pSqlTaskList[i].szGuarderId));
								pDev->ulBindTime = ulTaskStartTime;
							}
							pthread_mutex_unlock(&g_mutex4DevList);
							pthread_mutex_lock(&g_mutex4GuarderList);
							Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pSqlTaskList[i].szGuarderId);
							if (pGuarder) {
								pGuarder->uiState = STATE_GUARDER_DUTY;
								strncpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), pSqlTaskList[i].szDeviceId,
									strlen(pSqlTaskList[i].szDeviceId));
								strncpy_s(pTask->szOrg, sizeof(pTask->szOrg), pGuarder->szOrg, strlen(pGuarder->szOrg));
								strncpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), pSqlTaskList[i].szTaskId,
									strlen(pSqlTaskList[i].szTaskId));
								format_datetime(ulTaskStartTime, pGuarder->szTaskStartTime, sizeof(pGuarder->szTaskStartTime));
							}
							pthread_mutex_unlock(&g_mutex4GuarderList);

							memcpy_s(&pTaskList[i], nTaskSize, pTask, nTaskSize);
							
							pthread_mutex_lock(&g_mutex4TaskList);
							zhash_update(g_taskList, pTask->szTaskId, pTask);
							zhash_freefn(g_taskList, pTask->szTaskId, free);
							pthread_mutex_unlock(&g_mutex4TaskList);

							i++;
						}
						if (pQryFrom_) {
							replyQuery(pTaskList, nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_,
								pQryFrom_);
						}
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query row=%u, seq=%u, time=%lu, from=%s, "
							"sql=%s\r\n", __FUNCTION__, __LINE__, nCount, uiQrySeq_, ulQryTime_,
							pQryFrom_ ? pQryFrom_ : "", pSqlStatement_->pStatement);
						writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
						if (pSqlTaskList) {
							free(pSqlTaskList);
							pSqlTaskList = NULL;
						}
						if (pTaskList) {
							free(pTaskList);
							pTaskList = NULL;
						}
					}
					else if (pSqlStatement_->uiCorrelativeTable == escort_db::E_TBL_PERSON) {
						escort_db::SqlPerson * pSqlPersonList = (escort_db::SqlPerson *)zmalloc(nCount
							* sizeof(escort_db::SqlPerson));
						size_t nPersonSize = sizeof(Person);
						Person * pPersonList = (Person *)zmalloc(nCount * nPersonSize);
						int i = 0;
						while (row = mysql_fetch_row(res_ptr)) {
							strncpy_s(pSqlPersonList[i].szPersonId, sizeof(pSqlPersonList[i].szPersonId),
								row[0], strlen(row[0]));
							strncpy_s(pSqlPersonList[i].szPersonName, sizeof(pSqlPersonList[i].szPersonName),
								row[1], strlen(row[1]));
							pSqlPersonList[i].nFlee = atoi(row[2]);

							Person * pPerson = (Person *)zmalloc(nPersonSize);
							strncpy_s(pPerson->szPersonId, sizeof(pPerson->szPersonId), 
								pSqlPersonList[i].szPersonId, strlen(pSqlPersonList[i].szPersonId));
							strncpy_s(pPerson->szPersonName, sizeof(pPerson->szPersonName),
								pSqlPersonList[i].szPersonName, strlen(pSqlPersonList[i].szPersonName));
							pPerson->nFlee = pSqlPersonList[i].nFlee;

							memcpy_s(&pPersonList[i], nPersonSize, pPerson, nPersonSize);

							pthread_mutex_lock(&g_mutex4PersonList);
							zhash_update(g_personList, pPerson->szPersonId, pPerson);
							zhash_freefn(g_personList, pPerson->szPersonId, free);
							pthread_mutex_unlock(&g_mutex4PersonList);

							i++;
						}
						if (pQryFrom_ && strlen(pQryFrom_)) {
							replyQuery(pPersonList, nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_,
								pQryFrom_);
						}
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query row=%u, seq=%u, time=%lu, from=%s, "
							"sql=%s\r\n", __FUNCTION__, __LINE__, nCount, uiQrySeq_, ulQryTime_,
							pQryFrom_ ? pQryFrom_ : "", pSqlStatement_->pStatement);
						writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
						if (pSqlPersonList) {
							free(pSqlPersonList);
							pSqlPersonList = NULL;
						}
						if (pPersonList) {
							free(pPersonList);
							pPersonList = NULL;
						}
					}
					else if (pSqlStatement_->uiCorrelativeTable == escort_db::E_TBL_ORG) {
						escort_db::SqlOrg * pSqlOrgList = (escort_db::SqlOrg *)zmalloc(sizeof(escort_db::SqlOrg)
							* nCount);
						size_t nOrgSize = sizeof(Organization);
						Organization * pOrgList = (Organization *)zmalloc(nCount * nOrgSize);
						int i = 0;
						while (row = mysql_fetch_row(res_ptr)) {
							strncpy_s(pSqlOrgList[i].szOrgId, sizeof(pSqlOrgList[i].szOrgId), row[0], strlen(row[0]));
							strncpy_s(pSqlOrgList[i].szOrgName, sizeof(pSqlOrgList[i].szOrgName), row[1],
								strlen(row[1]));
							strncpy_s(pSqlOrgList[i].szParentOrgId, sizeof(pSqlOrgList[i].szParentOrgId),
								row[2], strlen(row[2]));
							Organization * pOrg = (Organization *)zmalloc(nOrgSize);
							strncpy_s(pOrg->szOrgId, sizeof(pOrg->szOrgId), pSqlOrgList[i].szOrgId, 
								strlen(pSqlOrgList[i].szOrgId));
							strncpy_s(pOrg->szOrgName, sizeof(pOrg->szOrgName), pSqlOrgList[i].szOrgName,
								strlen(pSqlOrgList[i].szOrgName));
							strncpy_s(pOrg->szParentOrgId, sizeof(pOrg->szParentOrgId), pSqlOrgList[i].szParentOrgId,
								strlen(pSqlOrgList[i].szParentOrgId));

							memcpy_s(&pOrgList[i], nOrgSize, pOrg, nOrgSize);

							pthread_mutex_lock(&g_mutex4OrgList);
							zhash_update(g_orgList, pOrg->szOrgId, pOrg);
							zhash_freefn(g_orgList, pOrg->szOrgId, free);
							pthread_mutex_unlock(&g_mutex4OrgList);

							i++;
						}
						if (pQryFrom_ && strlen(pQryFrom_)) {
							replyQuery(pOrgList, nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_,
								pQryFrom_);
						}
						if (pSqlOrgList) {
							free(pSqlOrgList);
							pSqlOrgList = NULL;
						}
						if (pOrgList) {
							free(pOrgList);
							pOrgList = NULL;
						}
					}
				}
				else { //rowCount = 0;
					if (pQryFrom_ && strlen(pQryFrom_)) {
						replyQuery(NULL, 0, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_, pQryFrom_);
					}
					snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query row is empty, seq=%u, time=%lu, from="
						"%s, sql=%s\r\n", __FUNCTION__, __LINE__, uiQrySeq_, ulQryTime_,
						pQryFrom_ ? pQryFrom_ : "", pSqlStatement_->pStatement);
					writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
				}
				mysql_free_result(res_ptr);
			}
			else { //res_ptr query null
				if (pQryFrom_ && strlen(pQryFrom_)) {
					replyQuery(NULL, 0, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_, pQryFrom_);
				}
				snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]get store error=%d,%s, seq=%u, time=%lu, from=%s, "
					"sql=%s\r\n", __FUNCTION__, __LINE__, mysql_errno(m_readConn), mysql_error(m_readConn),
					uiQrySeq_, ulQryTime_, pQryFrom_, pSqlStatement_->pStatement);
				writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
			}
		}
		else { //query error
			if (pQryFrom_ && strlen(pQryFrom_)) {
				replyQuery(NULL, 0, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_, pQryFrom_);
			}
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query error=%u,%s, seq=%u, time=%lu, from=%s, sql="
				"%s\r\n", __FUNCTION__, __LINE__, mysql_errno(m_readConn), mysql_error(m_readConn), uiQrySeq_, 
				ulQryTime_, pQryFrom_ ? pQryFrom_ : "", pSqlStatement_->pStatement);
			writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
		}
	}
	else { //conn error
		if (pQryFrom_ && strlen(pQryFrom_)) {
			replyQuery(NULL, 0, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_, pQryFrom_);
		}
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]conn error, seq=%u, time=%lu, from=%s, sql=%s\r\n",
			__FUNCTION__, __LINE__, uiQrySeq_, ulQryTime_, pQryFrom_ ? pQryFrom_ : "", 
			(pSqlStatement_ && pSqlStatement_->pStatement) ? pSqlStatement_->pStatement : "");
		writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
	}
	pthread_mutex_unlock(&m_mutex4ReadConn);
}

void DbProxy::handleSqlExec(dbproxy::SqlStatement * pSqlStatement_, unsigned int uiExecSeq_, 
	unsigned long ulExecTime_, const char * pExecFrom_)
{
	char szLog[1024] = { 0 };
	if (m_writeConn && pSqlStatement_ && pSqlStatement_->pStatement && pSqlStatement_->ulStatementLen) {
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]seq=%u, time=%lu, execute sql=%s\r\n",
			__FUNCTION__, __LINE__, uiExecSeq_, ulExecTime_, pSqlStatement_->pStatement);
		//writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
		printf(szLog);
		int nErr = mysql_real_query(m_writeConn, pSqlStatement_->pStatement, pSqlStatement_->ulStatementLen);
		if (nErr == 0) {
			my_ulonglong nAffectedRow = mysql_affected_rows(m_writeConn);
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute sql affected=%d, seq=%u, time=%lu, from=%s,"
				"sql=%s\r\n", __FUNCTION__, __LINE__, (int)nAffectedRow, uiExecSeq_, ulExecTime_,
				pExecFrom_ ? pExecFrom_ : "", pSqlStatement_->pStatement);
			writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
		}
		else {
			//need backup
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute sql error=%d, seq=%u, time=%lu, from=%s, sql"
				"=%s\r\n", __FUNCTION__, __LINE__, mysql_errno(m_writeConn), uiExecSeq_, ulExecTime_,
				pExecFrom_ ? pExecFrom_ : "", pSqlStatement_->pStatement);
			writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
		}
	}
	else {
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]write conn error, seq=%u, time=%lu, from=%s, sql=%s\r\n",
			__FUNCTION__, __LINE__, uiExecSeq_, ulExecTime_, pExecFrom_ ? pExecFrom_ : "",
			(pSqlStatement_ && pSqlStatement_->pStatement) ? pSqlStatement_->pStatement : "");
		writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
	}
}

void DbProxy::handleSqlLocate(dbproxy::SqlStatement * pSqlStatement_, unsigned int uiLocateSeq_, 
	unsigned long ulLocateTime_)
{
	char szLog[512] = { 0 };
	if (m_locateConn && pSqlStatement_ && pSqlStatement_->pStatement && pSqlStatement_->ulStatementLen) {
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]seq=%u, time=%lu, execute locate sql=%s\r\n",
			__FUNCTION__, __LINE__, uiLocateSeq_, ulLocateTime_, pSqlStatement_->pStatement);
		//writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
		printf(szLog);
		int nErr = mysql_real_query(m_locateConn, pSqlStatement_->pStatement, pSqlStatement_->ulStatementLen);
		if (nErr == 0) {
			my_ulonglong nAffectedRow = mysql_affected_rows(m_writeConn);
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute sql affected=%d, seq=%u, time=%lu, sql=%s\r\n",
				__FUNCTION__, __LINE__, (int)nAffectedRow, uiLocateSeq_, ulLocateTime_, pSqlStatement_->pStatement);
			writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
		}
		else {
			//backup
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]locate conn error=%d, seq=%u, time=%lu, sql=%s\r\n",
				__FUNCTION__, __LINE__, mysql_errno(m_locateConn), uiLocateSeq_, ulLocateTime_,
				pSqlStatement_->pStatement);
			writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
		}
	}
	else {
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]locate conn error, seq=%u, time=%lu, sql=%s\r\n",
			__FUNCTION__, __LINE__, uiLocateSeq_, ulLocateTime_, 
			(pSqlStatement_ && pSqlStatement_->pStatement) ? pSqlStatement_->pStatement : "");
		writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
	}
}

void DbProxy::replyQuery(void * pReplyData_, unsigned int uiReplyCount_, unsigned int uiCorrelativeTable_,
	unsigned int uiQrySeq_, unsigned long ulQryTime_, const char * pQryFrom_)
{
	if (pQryFrom_) {
		if (pReplyData_ && uiReplyCount_) {
			escort_db::SqlContainer container;
			container.uiSqlOptSeq = uiQrySeq_;
			container.ulSqlOptTime = ulQryTime_;
			container.usSqlOptTarget = uiCorrelativeTable_;
			container.usSqlOptType = escort_db::E_OPT_QUERY;
			container.szSqlOptKey[0] = '\0';
			container.uiResultCount = uiReplyCount_;
			container.uiResultLen = 0;
			switch (uiCorrelativeTable_) {
				case escort_db::E_TBL_PERSON: {
					Person * pPersonList = (Person *)pReplyData_;
					container.uiResultLen = sizeof(Person) * uiReplyCount_;
					container.pStoreResult = (unsigned char *)zmalloc(container.uiResultLen + 1);
					memcpy_s(container.pStoreResult, container.uiResultLen, pPersonList, container.uiResultLen);
					container.pStoreResult[container.uiResultLen] = '\0';
					break;
				}
				case escort_db::E_TBL_GUARDER: {
					Guarder * pGuarderList = (Guarder *)pReplyData_;
					container.uiResultLen = sizeof(Guarder) * uiReplyCount_;
					container.pStoreResult = (unsigned char *)zmalloc(container.uiResultLen + 1);
					memcpy_s(container.pStoreResult, container.uiResultLen, pGuarderList,
						container.uiResultLen);
					container.pStoreResult[container.uiResultLen] = '\0';
					break;
				}
				case escort_db::E_TBL_DEVICE: {
					WristletDevice * pDevList = (WristletDevice *)pReplyData_;
					container.uiResultLen = sizeof(WristletDevice) * uiReplyCount_;
					container.pStoreResult = (unsigned char *)zmalloc(container.uiResultLen + 1);
					memcpy_s(container.pStoreResult, container.uiResultLen, pDevList, container.uiResultLen);
					container.pStoreResult[container.uiResultLen] = '\0';
					break;
				}
				case escort_db::E_TBL_TASK: {
					EscortTask * pTaskList = (EscortTask *)pReplyData_;
					container.uiResultLen = sizeof(EscortTask) * uiReplyCount_;
					container.pStoreResult = (unsigned char *)zmalloc(container.uiResultLen + 1);
					memcpy_s(container.pStoreResult, container.uiResultLen, pTaskList, container.uiResultLen);
					container.pStoreResult[container.uiResultLen] = '\0';
					break;
				}
				case escort_db::E_TBL_ORG: {
					Organization * pOrgList = (Organization *)pReplyData_;
					container.uiResultLen = sizeof(Organization) * uiReplyCount_;
					container.pStoreResult = (unsigned char *)zmalloc(container.uiResultLen + 1);
					memcpy_s(container.pStoreResult, container.uiResultLen, pOrgList, container.uiResultLen);
					container.pStoreResult[container.uiResultLen] = '\0';
					break;
				}
			}
			if (container.uiResultLen) {
				zmsg_t * msg_reply = zmsg_new();
				zframe_t * frame_identity = zframe_from(pQryFrom_);
				zframe_t * frame_empty = zframe_new(NULL, 0);
				size_t nContainerSize = sizeof(escort_db::SqlContainer);
				size_t nFrameDataLen = nContainerSize + container.uiResultLen;
				unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
				memcpy_s(pFrameData, nContainerSize, &container, nContainerSize);
				memcpy_s(pFrameData + nContainerSize, container.uiResultLen + 1, container.pStoreResult,
					container.uiResultLen);
				pFrameData[nFrameDataLen] = '\0';
				zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
				zmsg_append(msg_reply, &frame_identity);
				zmsg_append(msg_reply, &frame_empty);
				zmsg_append(msg_reply, &frame_body);
				zmsg_send(&msg_reply, m_reception);
			}
		}
		else {
			zmsg_t * msg_reply = zmsg_new();
			zframe_t * frame_idenity = zframe_from(pQryFrom_);
			zframe_t * frame_empty = zframe_new(NULL, 0);
			escort_db::SqlContainer container;
			container.usSqlOptType = escort_db::E_OPT_QUERY;
			container.usSqlOptTarget = uiCorrelativeTable_;
			container.szSqlOptKey[0] = '\0';
			container.uiSqlOptSeq = uiQrySeq_;
			container.ulSqlOptTime = ulQryTime_;
			container.uiResultCount = 0;
			container.uiResultLen = 0;
			container.pStoreResult = NULL;
			size_t nContainerSize = sizeof(escort_db::SqlContainer);
			unsigned char * pFrameData = (unsigned char *)zmalloc(nContainerSize + 1);
			memcpy_s(pFrameData, nContainerSize, &container, nContainerSize);
			pFrameData[nContainerSize] = '\0';
			zframe_t * frame_body = zframe_new(pFrameData, nContainerSize);
			zmsg_append(msg_reply, &frame_idenity);
			zmsg_append(msg_reply, &frame_empty);
			zmsg_append(msg_reply, &frame_body);
			zmsg_send(&msg_reply, m_reception);
			free(pFrameData);
			pFrameData = NULL;
		}
	}
}

void DbProxy::dealNetwork()
{
	zmq_pollitem_t items[] = { {m_subscriber, 0, ZMQ_POLLIN, 0}, {m_interactor, 0, ZMQ_POLLIN, 0},
														 {m_reception, 0, ZMQ_POLLIN, 0}, {m_pipeline, 0, ZMQ_POLLIN, 0} };
	while (m_nRun) {
		int rc = zmq_poll(items, 4, 1000 * ZMQ_POLL_MSEC);
		if (rc == -1 && errno == ETERM) {
			break;
		}
		if (items[0].revents & ZMQ_POLLIN) { //Topic message
			zmsg_t * subMsg = zmsg_recv(items[0].socket);
			if (subMsg) {
				zframe_t * frame_mark = zmsg_pop(subMsg);
				zframe_t * frame_seq = zmsg_pop(subMsg);
				zframe_t * frame_type = zmsg_pop(subMsg);
				zframe_t * frame_uuid = zmsg_pop(subMsg);
				zframe_t * frame_body = zmsg_pop(subMsg);
				char * strMark = zframe_strdup(frame_mark);
				char * strSeq = zframe_strdup(frame_seq);
				char * strType = zframe_strdup(frame_type);
				char * strUuid = zframe_strdup(frame_uuid);
				char * strBody = zframe_strdup(frame_body);
				TopicMessage * pMsg = (TopicMessage *)zmalloc(sizeof(TopicMessage));
				if (pMsg) {
					strncpy_s(pMsg->szMsgMark, sizeof(pMsg->szMsgMark), strMark, strlen(strMark));
					strncpy_s(pMsg->szMsgUuid, sizeof(pMsg->szMsgUuid), strUuid, strlen(strUuid));
					strncpy_s(pMsg->szMsgBody, sizeof(pMsg->szMsgBody), strBody, strlen(strBody));
					pMsg->usMsgSequence = (unsigned short)atoi(strSeq);
					pMsg->usMsgType = (unsigned short)atoi(strType);
					if (!addTopicMsg(pMsg)) {
						free(pMsg);
						pMsg = NULL;
					}
				}
				zframe_destroy(&frame_mark);
				zframe_destroy(&frame_seq);
				zframe_destroy(&frame_type);
				zframe_destroy(&frame_uuid);
				zframe_destroy(&frame_body);
				zmsg_destroy(&subMsg);
				free(strMark);
				free(strSeq);
				free(strType);
				free(strUuid);
				free(strBody);
				zmsg_destroy(&subMsg);
			}
		}
		if (items[1].revents & ZMQ_POLLIN) { //interactor message
			zmsg_t * interactMsg = zmsg_recv(items[1].socket);
			if (interactMsg) {
				size_t nCount = zmsg_size(interactMsg);
				if (nCount) {
					zframe_t ** interactFrames = (zframe_t **)zmalloc(nCount * sizeof(zframe_t *));
					InteractionMessage * pMsg = (InteractionMessage *)zmalloc(sizeof(InteractionMessage));
					pMsg->uiContentCount = nCount;
					pMsg->pMsgContents = (char **)zmalloc(nCount * sizeof(char *));
					pMsg->uiContentLens = (unsigned int *)zmalloc(nCount * sizeof(unsigned int));
					for (size_t i = 0; i < nCount; i++) {
						interactFrames[i] = zmsg_pop(interactMsg);
						size_t nFrameLen = zframe_size(interactFrames[i]);
						pMsg->uiContentLens[i] = nFrameLen;
						pMsg->pMsgContents[i] = zframe_strdup(interactFrames[i]);
						zframe_destroy(&interactFrames[i]);
					}
					if (!addInteractMsg(pMsg)) {
						for (size_t i = 0; i < nCount; i++) {
							free(pMsg->pMsgContents[i]);
							pMsg->pMsgContents[i] = NULL;
						}
						free(pMsg->pMsgContents);
						pMsg->pMsgContents = NULL;
						free(pMsg->uiContentLens);
						pMsg->uiContentLens = NULL;
						free(pMsg);
						pMsg = NULL;
					}
					free(interactFrames);
					interactFrames = NULL;
				}
				zmsg_destroy(&interactMsg);
			}
		}
		if (items[2].revents & ZMQ_POLLIN) { //reception
			zmsg_t * receptMsg = zmsg_recv(items[2].socket);
			if (receptMsg) {
				size_t nFrameSize = zmsg_size(receptMsg);
				zframe_t * frame_identity = zmsg_pop(receptMsg);
				char * szIdentity = zframe_strdup(frame_identity);
				zframe_t * frame_empty = NULL;
				if (nFrameSize == 3) {
					frame_empty = zmsg_pop(receptMsg);
				}
				zframe_t * frame_body = zmsg_pop(receptMsg);
				size_t nBodySize = zframe_size(frame_body);
				size_t nContainerSize = sizeof(escort_db::SqlContainer);
				if (frame_body && nBodySize >= nContainerSize) {
					unsigned char * pFrameData = zframe_data(frame_body);
					escort_db::SqlContainer container;
					memcpy_s(&container, nContainerSize, pFrameData, nContainerSize);
					if (container.uiResultLen && container.uiResultLen <= nBodySize - nContainerSize) {
						container.pStoreResult = (unsigned char *)zmalloc(container.uiResultLen + 1);
						memcpy_s(container.pStoreResult, container.uiResultLen, pFrameData + nContainerSize,
							container.uiResultLen);
						container.pStoreResult[container.uiResultLen] = '\0';
					}
					handleReception(&container, szIdentity);
					free(szIdentity);
					zframe_destroy(&frame_body);
					if (frame_empty) {
						zframe_destroy(&frame_empty);
					}
					zframe_destroy(&frame_identity);
					zmsg_destroy(&receptMsg);
					if (container.pStoreResult && container.uiResultLen) {
						free(container.pStoreResult);
						container.pStoreResult = NULL;
						container.uiResultLen = 0;
					}
				}
			}
		} 
		else if (items[3].revents & ZMQ_POLLIN) {//pipeline
			zmsg_t * pipeMsg = zmsg_recv(m_pipeline);
			if (pipeMsg) {
				zframe_t * pipeFrame = zmsg_pop(pipeMsg);
				if (pipeFrame) {
					unsigned char * pFrameData = zframe_data(pipeFrame);
					size_t nFrameDataLen = zframe_size(pipeFrame);
					if (pFrameData && nFrameDataLen) {

					}
					zframe_destroy(&pipeFrame);
				}
				zmsg_destroy(&pipeMsg);
			}
		}
 	}
}

bool DbProxy::addTopicMsg(TopicMessage * pMsg_)
{
	bool result = false;
	if (pMsg_) {
		pthread_mutex_lock(&m_mutex4TopicMsgQue);
		m_topicMsgQue.push(pMsg_);
		if (m_topicMsgQue.size() == 1) {
			pthread_cond_signal(&m_cond4TopicMsgQue);
		}
		result = true;
		pthread_mutex_unlock(&m_mutex4TopicMsgQue);
	}
	return result;
}

void DbProxy::dealTopicMsg()
{
	char szLog[512] = { 0 };
	do {
		pthread_mutex_lock(&m_mutex4TopicMsgQue);
		while (m_nRun && m_topicMsgQue.empty()) {
			pthread_cond_wait(&m_cond4TopicMsgQue, &m_mutex4TopicMsgQue);
		}
		if (!m_nRun && m_topicMsgQue.empty()) {
			pthread_mutex_unlock(&m_mutex4TopicMsgQue);
			break;
		}
		TopicMessage * pMsg = m_topicMsgQue.front();
		m_topicMsgQue.pop();
		pthread_mutex_unlock(&m_mutex4TopicMsgQue);
		if (pMsg) {
			switch (pMsg->usMsgType) {
				case MSG_DEVICE_ALIVE: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						TopicAliveMessage aliveMsg;
						bool bValidFactory = false;
						bool bValidDevice = false;
						bool bValidDatetime = false;
						char szDatetime[20] = { 0 };
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString()) {
								size_t nSize = doc["factoryId"].GetStringLength();
								if (nSize) {
									strncpy_s(aliveMsg.szFactoryId, sizeof(aliveMsg.szFactoryId), 
										doc["factoryId"].GetString(), nSize);
									bValidFactory = true;
								}
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString()) {
								size_t nSize = doc["deviceId"].GetStringLength();
								if (nSize) {
									strncpy_s(aliveMsg.szDeviceId, sizeof(aliveMsg.szDeviceId),
										doc["deviceId"].GetString(), nSize);
									bValidDevice = true;
								}
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString()) {
								size_t nSize = doc["orgId"].GetStringLength();
								if (nSize) {
									strncpy_s(aliveMsg.szOrg, sizeof(aliveMsg.szOrg),
										doc["orgId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("battery")) {
							if (doc["battery"].IsInt()) {
								int nBattery = doc["battery"].GetInt();
								if (nBattery < 0 || nBattery > 100) {
									nBattery = 0;
								}
								aliveMsg.usBattery = (unsigned short)nBattery;
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString()) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
									aliveMsg.ulMessageTime = strdatetime2time(szDatetime);
									bValidDatetime = true;
								}
							}
						}
						if (bValidFactory && bValidDevice && bValidDatetime) {
							handleTopicAliveMsg(&aliveMsg);
							storeTopicMsg(pMsg, aliveMsg.ulMessageTime);
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_ALIVE_MSG data miss, uuid=%s, seq"
								"=%u, factoryId=%s, deviceId=%s, orgId=%s, battery=%u, datetime=%s\r\n",
								__FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence, aliveMsg.szFactoryId,
								aliveMsg.szDeviceId, aliveMsg.szOrg, aliveMsg.usBattery, 
								bValidDatetime ? szDatetime : "null");
							writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProx]%s[%d]parse MSG_DEVICE_ALIVE msg data uuid=%s"
							", seq=%u, parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, 
							pMsg->usMsgSequence);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_DEVICE_ONLINE: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						TopicOnlineMessage onlineMsg;
						bool bValidFactory = false;
						bool bValidDevice = false;
						bool bValidDatetime = false;
						char szDatetime[20] = { 0 };
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString()) {
								size_t nSize = doc["factoryId"].GetStringLength();
								if (nSize) {
									strncpy_s(onlineMsg.szFactoryId, sizeof(onlineMsg.szFactoryId),
										doc["factoryId"].GetString(), nSize);
									bValidFactory = true;
								}
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString()) {
								size_t nSize = doc["deviceId"].GetStringLength();
								if (nSize) {
									strncpy_s(onlineMsg.szDeviceId, sizeof(onlineMsg.szDeviceId),
										doc["deviceId"].GetString(), nSize);
									bValidDevice = true;
								}
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString()) {
								size_t nSize = doc["orgId"].GetStringLength();
								if (nSize) {
									strncpy_s(onlineMsg.szOrg, sizeof(onlineMsg.szOrg),
										doc["orgId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("battery")) {
							if (doc["battery"].IsInt()) {
								int nBattery = doc["battery"].GetInt();
								if (nBattery < 0 || nBattery > 100) {
									nBattery = 0;
								}
								onlineMsg.usBattery = (unsigned short)nBattery;
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString()) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
									onlineMsg.ulMessageTime = strdatetime2time(szDatetime);
									bValidDatetime = true;
								}
							}
						}
						if (bValidFactory && bValidDevice && bValidDatetime) {
							handleTopicOnlineMsg(&onlineMsg);
							storeTopicMsg(pMsg, onlineMsg.ulMessageTime);
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_ALIVE_MSG data miss, uuid=%s, seq"
								"=%u, factoryId=%s, deviceId=%s, orgId=%s, battery=%u, datetime=%s\r\n",
								__FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence, 
								bValidFactory ? onlineMsg.szFactoryId : "null", onlineMsg.szDeviceId, onlineMsg.szOrg,
								onlineMsg.usBattery, bValidDatetime ? szDatetime : "null");
							writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_ONLINE msg data uuid=%s, "
							"seq=%u, parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid,
							pMsg->usMsgSequence);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_DEVICE_OFFLINE: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						TopicOfflineMessage offlineMsg;
						bool bValidFactory = false;
						bool bValidDevice = false;
						bool bValidDatetime = false;
						char szDatetime[20] = { 0 };
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString()) {
								size_t nSize = doc["factoryId"].GetStringLength();
								if (nSize) {
									strncpy_s(offlineMsg.szFactoryId, sizeof(offlineMsg.szFactoryId),
										doc["factoryId"].GetString(), nSize);
									bValidFactory = true;
								}
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString()) {
								size_t nSize = doc["deviceId"].GetStringLength();
								if (nSize) {
									strncpy_s(offlineMsg.szDeviceId, sizeof(offlineMsg.szDeviceId),
										doc["deviceId"].GetString(), nSize);
									bValidDevice = true;
								}
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString()) {
								size_t nSize = doc["orgId"].GetStringLength();
								if (nSize) {
									strncpy_s(offlineMsg.szOrg, sizeof(offlineMsg.szOrg),
										doc["orgId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString()) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
									offlineMsg.ulMessageTime = strdatetime2time(szDatetime);
									bValidDatetime = true;
								}
							}
						}
						if (bValidFactory && bValidDevice && bValidDatetime) {
							handleTopicOfflineMsg(&offlineMsg);
							storeTopicMsg(pMsg, offlineMsg.ulMessageTime);
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_DEVICE_OFFLINE data miss, uuid=%s, "
								"seq=%u, factoryId=%s, deviceId=%s, datetime=%s\r\n", __FUNCTION__, __LINE__,
								pMsg->szMsgUuid, pMsg->usMsgSequence, bValidFactory ? offlineMsg.szFactoryId : "null",
								bValidDevice ? offlineMsg.szDeviceId : "null", 
								bValidDatetime ? szDatetime : "null");
							writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_OFFLINE msg data, uuid=%s"
							", seq=%u, parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, 
							pMsg->usMsgSequence);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_DEVICE_LOCATE: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						int nSubType = -1;
						if (doc.HasMember("subType")) {
							if (doc["subType"].IsInt()) {
								nSubType = doc["subType"].GetInt();
							}
						}
						switch (nSubType) {
							case LOCATE_GPS: {
								TopicLocateMessageGps gpsLocateMsg;
								bool bValidFlag = false;
								bool bValidFactory = false;
								bool bValidDevice = false;
								bool bValidLat = false;
								bool bValidLng = false;
								bool bValidDatetime = false;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("locateFlag")) {
									if (doc["locateFlag"].IsInt()) {
										gpsLocateMsg.usFlag = (unsigned short)doc["locateFlag"].GetInt();
										bValidFlag = true;
									}
								}
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(gpsLocateMsg.szFactoryId, sizeof(gpsLocateMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
											bValidFactory = true;
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(gpsLocateMsg.szDeviceId, sizeof(gpsLocateMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
											bValidDevice = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(gpsLocateMsg.szOrg, sizeof(gpsLocateMsg.szOrg),
												doc["orgId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										double d = doc["latitude"].GetDouble();
										if (d > 0) {
											gpsLocateMsg.dLat = d;
											bValidLat = true;
										}
									}
								}
								if (doc.HasMember("latType")) {
									if (doc["latType"].IsInt()) {
										gpsLocateMsg.usLatType = (unsigned short)doc["latType"].GetInt();
									}
								}
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										double d = doc["lngitude"].GetDouble();
										if (d > 0) {
											gpsLocateMsg.dLng = d;
											bValidLng = true;
										}
									}
								}
								if (doc.HasMember("lngType")) {
									if (doc["lngType"].IsInt()) {
										gpsLocateMsg.usLngType = (unsigned short)doc["lngType"].GetInt();
									}
								}
								if (doc.HasMember("stattelite")) {
									if (doc["stattelite"].IsInt()) {
										gpsLocateMsg.usStattelite = (unsigned short)doc["sattelite"].GetInt();
									}
								}
								if (doc.HasMember("intensity")) {
									if (doc["intensity"].IsInt()) {
										gpsLocateMsg.usIntensity = (unsigned short)doc["intensity"].GetInt();
									}
								}
								if (doc.HasMember("speed")) {
									if (doc["speed"].IsDouble()) {
										gpsLocateMsg.dSpeed = doc["speed"].GetDouble();
									}
								}
								if (doc.HasMember("direction")) {
									if (doc["direction"].IsInt()) {
										gpsLocateMsg.nDirection = doc["direction"].GetInt();
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										gpsLocateMsg.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											gpsLocateMsg.ulMessageTime = strdatetime2time(szDatetime);
											bValidDatetime = true;
										}
									}
								}
								if (bValidFlag && bValidFactory && bValidDevice && bValidLat && bValidLng && bValidDatetime) {
									handleTopicGpsLocateMsg(&gpsLocateMsg);
									storeTopicMsg(pMsg, gpsLocateMsg.ulMessageTime);
								}
								else {
									snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_DEVICE_LOCATE gps locate msg data miss, "
										"uuid=%s, seq=%u, factoryId=%s, deviceId=%s, orgId=%s, lat=%.06f|%u, lng=%.06f|%u, flag="
										"%u, sattelite=%u, intensity=%u, speed=%.04f, direction=%d, battery=%u, datetime=%s\r\n",
										__FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence,
										bValidFactory ? gpsLocateMsg.szFactoryId : "null",
										bValidDevice ? gpsLocateMsg.szDeviceId : "null",
										gpsLocateMsg.szOrg, gpsLocateMsg.dLat, gpsLocateMsg.usLatType, gpsLocateMsg.dLng,
										gpsLocateMsg.usLngType, gpsLocateMsg.usFlag, gpsLocateMsg.usStattelite,
										gpsLocateMsg.usIntensity, gpsLocateMsg.dSpeed, gpsLocateMsg.nDirection,
										gpsLocateMsg.usBattery, bValidDatetime ? szDatetime : "null");
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case LOCATE_LBS: {
								TopicLocateMessageLbs lbsLocateMsg;
								bool bValidFlag = false;
								bool bValidFactory = false;
								bool bValidDevice = false;
								bool bValidLat = false;
								bool bValidLng = false;
								bool bValidDatetime = false;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("locateFlag")) {
									if (doc["locateFlag"].IsInt()) {
										lbsLocateMsg.usFlag = (unsigned short)doc["locateFlag"].GetInt();
										bValidFlag = true;
									}
								}
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(lbsLocateMsg.szFactoryId, sizeof(lbsLocateMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
											bValidFactory = true;
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(lbsLocateMsg.szDeviceId, sizeof(lbsLocateMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
											bValidDevice = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(lbsLocateMsg.szOrg, sizeof(lbsLocateMsg.szOrg),
												doc["orgId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										lbsLocateMsg.dLat = doc["latitude"].GetDouble();
										bValidLat = true;
									}
								}
								if (doc.HasMember("latType")) {
									if (doc["latType"].IsInt()) {
										lbsLocateMsg.usLatType = (unsigned short)doc["latType"].GetInt();
									}
								}
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										lbsLocateMsg.dLng = doc["lngitude"].GetDouble();
										bValidLng = true;
									}
								}
								if (doc.HasMember("lngType")) {
									if (doc["lngType"].IsInt()) {
										lbsLocateMsg.usLngType = (unsigned short)doc["lngType"].GetInt();
									}
								}
								if (doc.HasMember("precision")) {
									if (doc["precision"].IsInt()) {
										lbsLocateMsg.nPrecision = doc["precision"].GetInt();
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										lbsLocateMsg.usBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											lbsLocateMsg.ulMessageTime = strdatetime2time(szDatetime);
											bValidDatetime = true;
										}
									}
								}
								if (bValidDatetime && bValidFlag && bValidFactory && bValidDevice && bValidLat && bValidLng) {
									handleTopicLbsLocateMsg(&lbsLocateMsg);
									storeTopicMsg(pMsg, lbsLocateMsg.ulMessageTime);
								}
								else {
									snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_DEVICE_LOCATE lbs locate msg data miss, "
										"uuid=%s, seq=%u, flag=%u, factoryId=%s, deviceId=%s, orgId=%s, lat=%.06f|%u, lng=%.06f|%u,"
										 " precision=%d, battery=%u, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid,
										pMsg->usMsgSequence, lbsLocateMsg.usFlag, bValidFactory ? lbsLocateMsg.szFactoryId : "null",
										bValidDevice ? lbsLocateMsg.szDeviceId : "null", lbsLocateMsg.szOrg, lbsLocateMsg.dLat,
										lbsLocateMsg.usLatType, lbsLocateMsg.dLng, lbsLocateMsg.usLngType, lbsLocateMsg.nPrecision,
										lbsLocateMsg.usBattery, bValidDatetime ? szDatetime : "null");
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case LOCATE_BT: {
								TopicLocateMessagBt btLocateMsg;
								bool bValidFactory = false;
								bool bValidDevice = false;
								bool bValidOrg = false;
								bool bValidLat = false;
								bool bValidLng = false;
								bool bValidTask = false;
								bool bValidBattery = false;
								bool bValidDatetime = false;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(btLocateMsg.szFactoryId, sizeof(btLocateMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
											bValidFactory = true;
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(btLocateMsg.szDeviceId, sizeof(btLocateMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
											bValidDevice = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(btLocateMsg.szOrg, sizeof(btLocateMsg.szOrg),
												doc["orgId"].GetString(), nSize);
											bValidOrg = true;
										}
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										double d = doc["latitude"].GetDouble();
										if (d > 0) {
											btLocateMsg.dLat = d;
											bValidLat = true;
										}
									}
								}
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										double d = doc["lngitude"].GetDouble();
										if (d > 0) {
											btLocateMsg.dLng = d;
											bValidLng = true;
										}
									}
								}
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strncpy_s(btLocateMsg.szTaskId, sizeof(btLocateMsg.szTaskId),
												doc["taskId"].GetString(), nSize);
											bValidTask = true;
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										btLocateMsg.usBattery = (unsigned short)doc["battery"].GetInt();
										bValidBattery = true;
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											btLocateMsg.ulMessageTime = strdatetime2time(szDatetime);
											bValidDatetime = true;
										}
									}
								}
								if (bValidFactory && bValidDevice && bValidOrg && bValidTask && bValidLat && bValidLng
									&& bValidBattery && bValidDatetime) {
									handleTopicBtLocateMsg(&btLocateMsg);
									storeTopicMsg(pMsg, btLocateMsg.ulMessageTime);
								}
								else {
									snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_DEVICE_LOCATE msg data miss, uuid=%s,"
										" seq=%u, factoryId=%s, deviceId=%s, taskId=%s, orgId=%s, lat=%.06f, lng=%.06f, "
										"datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence,
										bValidFactory ? btLocateMsg.szFactoryId : "null",
										bValidDevice ? btLocateMsg.szDeviceId : "null",
										bValidTask ? btLocateMsg.szTaskId : "null", bValidOrg ? btLocateMsg.szOrg : "null",
										btLocateMsg.dLat, btLocateMsg.dLng, bValidDatetime ? szDatetime : "null");
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_LOCATE msg data, subType="
									"%d, uuid=%s, seq=%u, error\r\n", __FUNCTION__, __LINE__, nSubType, pMsg->szMsgUuid,
									pMsg->usMsgSequence);
								writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_LOCATE msg data, uuid=%s, seq=%u"
							", parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_DEVICE_ALARM: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						int nSubType = -1;
						if (doc.HasMember("subType")) {
							if (doc["subType"].IsInt()) {
								nSubType = doc["subType"].GetInt();
							}
						}
						switch (nSubType) {
							case ALARM_DEVICE_LOWPOWER: {
								TopicAlarmMessageLowpower lowpowerAlarmMsg;
								char szDatetime[20] = { 0 };
								bool bValidFactory = false;
								bool bValidDevice = false;
								bool bValidMode = false;
								bool bValidBattery = false;
								bool bValidDatetime = false;
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(lowpowerAlarmMsg.szFactoryId, sizeof(lowpowerAlarmMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
											bValidFactory = true;
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(lowpowerAlarmMsg.szDeviceId, sizeof(lowpowerAlarmMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
											bValidDevice = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(lowpowerAlarmMsg.szOrg, sizeof(lowpowerAlarmMsg.szOrg),
												doc["orgId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										int nBattery = doc["battery"].GetInt();
										if (nBattery < 0 && nBattery > 100) {
											nBattery = 0;
										}
										lowpowerAlarmMsg.usBattery = (unsigned short)nBattery;
										bValidBattery = true;
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										int nMode = doc["mode"].GetInt();
										if (nMode != 0) {
											nMode = 1;
										}
										lowpowerAlarmMsg.usMode = (unsigned short)nMode;
										bValidMode = true;
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											lowpowerAlarmMsg.ulMessageTime = strdatetime2time(szDatetime);
											bValidDatetime = true;
										}
									}
								}
								if (bValidFactory && bValidDevice && bValidMode && bValidBattery && bValidDatetime) {
									handleTopicLowpoweAlarmMsg(&lowpowerAlarmMsg);
									storeTopicMsg(pMsg, lowpowerAlarmMsg.ulMessageTime);
								}
								else {
									snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]low power alarm msg data miss, uuid=%s, "
										"seq=%u, factoryId=%s, deviceId=%s, orgId=%s, mode=%u, battery=%u, datetime=%s\r\n",
										__FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence,
										bValidFactory ? lowpowerAlarmMsg.szFactoryId : "null",
										bValidDevice ? lowpowerAlarmMsg.szDeviceId : "null", lowpowerAlarmMsg.szOrg,
										lowpowerAlarmMsg.usMode, lowpowerAlarmMsg.usBattery,
										bValidDatetime ? szDatetime : "null");
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case ALARM_DEVICE_LOOSE: {
								TopicAlarmMessageLoose looseAlarmMsg;
								char szDatetime[20] = { 0 };
								bool bValidFactory = false;
								bool bValidDevice = false;
								bool bValidMode = false;
								bool bValidBattery = false;
								bool bValidDatetime = false;
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(looseAlarmMsg.szFactoryId, sizeof(looseAlarmMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
											bValidFactory = true;
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(looseAlarmMsg.szDeviceId, sizeof(looseAlarmMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
											bValidDevice = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(looseAlarmMsg.szOrg, sizeof(looseAlarmMsg.szOrg),
												doc["orgId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										int nBattery = doc["battery"].GetInt();
										if (nBattery < 0 && nBattery > 100) {
											nBattery = 0;
										}
										looseAlarmMsg.usBattery = (unsigned short)nBattery;
										bValidBattery = true;
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										int nMode = doc["mode"].GetInt();
										if (nMode != 0) {
											nMode = 1;
										}
										looseAlarmMsg.usMode = (unsigned short)nMode;
										bValidMode = true;
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											looseAlarmMsg.ulMessageTime = strdatetime2time(szDatetime);
											bValidDatetime = true;
										}
									}
								}
								if (bValidFactory && bValidDevice && bValidMode && bValidBattery && bValidDatetime) {
									handleTopicLooseAlarmMsg(&looseAlarmMsg);
									storeTopicMsg(pMsg, looseAlarmMsg.ulMessageTime);
								}
								else {
									snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]loose alarm msg data miss, uuid=%s, "
										"seq=%u, factoryId=%s, deviceId=%s, orgId=%s, mode=%u, battery=%u, datetime=%s\r\n",
										__FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence,
										bValidFactory ? looseAlarmMsg.szFactoryId : "null",
										bValidDevice ? looseAlarmMsg.szDeviceId : "null", looseAlarmMsg.szOrg,
										looseAlarmMsg.usMode, looseAlarmMsg.usBattery, bValidDatetime ? szDatetime : "null");
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							case ALARM_DEVICE_FLEE: {
								TopicAlarmMessageFlee fleeAlarmMsg;
								char szDatetime[20] = { 0 };
								bool bValidFactory = false;
								bool bValidDevice = false;
								bool bValidMode = false;
								bool bValidBattery = false;
								bool bValidGuarder = false;
								bool bValidTask = false;
								bool bValidOrg = false;
								bool bValidDatetime = false;
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(fleeAlarmMsg.szFactoryId, sizeof(fleeAlarmMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
											bValidFactory = true;
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(fleeAlarmMsg.szDeviceId, sizeof(fleeAlarmMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
											bValidDevice = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(fleeAlarmMsg.szOrg, sizeof(fleeAlarmMsg.szOrg),
												doc["orgId"].GetString(), nSize);
											bValidOrg = true;
										}
									}
								}
								if (doc.HasMember("guarder")) {
									if (doc["guarder"].IsString()) {
										size_t nSize = doc["guarder"].GetStringLength();
										if (nSize) {
											strncpy_s(fleeAlarmMsg.szGuarder, sizeof(fleeAlarmMsg.szGuarder),
												doc["guarder"].GetString(), nSize);
											bValidGuarder = true;
										}
									}
								}
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strncpy_s(fleeAlarmMsg.szTaskId, sizeof(fleeAlarmMsg.szTaskId),
												doc["taskId"].GetString(), nSize);
											bValidTask = true;
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										int nBattery = doc["battery"].GetInt();
										if (nBattery < 0 && nBattery > 100) {
											nBattery = 0;
										}
										fleeAlarmMsg.usBattery = (unsigned short)nBattery;
										bValidBattery = true;
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										int nMode = doc["mode"].GetInt();
										if (nMode != 0) {
											nMode = 1;
										}
										fleeAlarmMsg.usMode = (unsigned short)nMode;
										bValidMode = true;
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											fleeAlarmMsg.ulMessageTime = strdatetime2time(szDatetime);
											bValidDatetime = true;
										}
									}
								}
								if (bValidFactory && bValidDevice && bValidMode && bValidBattery && bValidDatetime 
										&& bValidGuarder && bValidOrg && bValidTask) {
									handleTopicFleeAlarmMsg(&fleeAlarmMsg);
									storeTopicMsg(pMsg, fleeAlarmMsg.ulMessageTime);
								}
								else {
									snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]loose alarm msg data miss, uuid=%s, "
										"seq=%u, factoryId=%s, deviceId=%s, orgId=%s, guarder=%s, taskId=%s, mode=%d, "
										"battery=%u, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid,
										pMsg->usMsgSequence, bValidFactory ? fleeAlarmMsg.szFactoryId : "null",
										bValidDevice ? fleeAlarmMsg.szDeviceId : "null",
										bValidGuarder ? fleeAlarmMsg.szGuarder : "null",
										bValidTask ? fleeAlarmMsg.szTaskId : "null",
										bValidOrg ? fleeAlarmMsg.szOrg : "null", bValidMode ? fleeAlarmMsg.usMode : -1,
										fleeAlarmMsg.usBattery, bValidDatetime ? szDatetime : "null");
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_DEVICE_ALARM unsupport type=%d, uuid=%s,"
									" seq=%u\r\n", __FUNCTION__, __LINE__, nSubType, pMsg->szMsgUuid, pMsg->usMsgSequence);
								writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_ALARM msg data, uuid=%s, seq=%u"
							", parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_DEVICE_BIND: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						TopicBindMessage bindMsg;
						bool bValidFactory = false;
						bool bValidDevice = false;
						bool bValidGuarder = false;
						bool bValidMode = false;
						bool bValidDatetime = false;
						char szDatetime[20] = { 0 };
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString()) {
								size_t nSize = doc["factoryId"].GetStringLength();
								if (nSize) {
									strncpy_s(bindMsg.szFactoryId, sizeof(bindMsg.szFactoryId), 
										doc["factoryId"].GetString(), nSize);
									bValidFactory = true;
								}
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString()) {
								size_t nSize = doc["deviceId"].GetStringLength();
								if (nSize) {
									strncpy_s(bindMsg.szDeviceId, sizeof(bindMsg.szDeviceId), 
										doc["deviceId"].GetString(), nSize);
									bValidDevice = true;
								}
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString()) {
								size_t nSize = doc["orgId"].GetStringLength();
								if (nSize) {
									strncpy_s(bindMsg.szOrg, sizeof(bindMsg.szOrg), doc["orgId"].GetString(), nSize);
								}
							}
						}
						if (doc.HasMember("guarder")) {
							if (doc["guarder"].IsString()) {
								size_t nSize = doc["guarder"].GetStringLength();
								if (nSize) {
									strncpy_s(bindMsg.szGuarder, sizeof(bindMsg.szGuarder), doc["guarder"].GetString(), nSize);
									bValidGuarder = true;
								}
							}
						}
						if (doc.HasMember("mode")) {
							if (doc["mode"].IsInt()) {
								bindMsg.usMode = (unsigned short)doc["mode"].GetInt();
								bValidMode = true;
							}
						}
						if (doc.HasMember("battery")) {
							if (doc["battery"].IsInt()) {
								int nBattery = doc["battery"].GetInt();
								if (nBattery >= 0 || nBattery <= 100) {
									bindMsg.usBattery = nBattery;
								}
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString()) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
									bindMsg.ulMessageTime = strdatetime2time(szDatetime);
									bValidDatetime = true;
								}
							}
						}
						if (bValidFactory && bValidDevice && bValidGuarder && bValidMode && bValidDatetime) {
							handleTopicBindMsg(&bindMsg);
							storeTopicMsg(pMsg, bindMsg.ulMessageTime);
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_DEVICE_BIND data miss, uuid=%s, seq=%u, "
								"factoryId=%s, deviceId=%s, orgId=%s, guarder=%s, mode=%d, datetime=%s\r\n", __FUNCTION__,
								__LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence, bValidFactory ? bindMsg.szFactoryId : "null",
								bValidDevice ? bindMsg.szDeviceId : "null", bindMsg.szOrg,
								bValidGuarder ? bindMsg.szGuarder : "null", bindMsg.usMode, 
								bValidDatetime ? szDatetime : "null");
							writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_BIND msg data, uuid=%s, seq=%u"
							", parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_TASK: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						int nState = -1;
						if (doc.HasMember("state")) {
							if (doc["state"].IsInt()) {
								nState = doc["state"].GetInt();
							}
						}
						if (nState != -1) {
							if (nState == 0) {
								TopicTaskMessage taskMsg;
								bool bValidTask = false;
								bool bValidFactory = false;
								bool bValidDevice = false;
								bool bValidOrg = false;
								bool bValidGuarder = false;
								bool bValidType = false;
								bool bValidLimit = false;
								bool bValidDatetime = false;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strncpy_s(taskMsg.szTaskId, sizeof(taskMsg.szTaskId), doc["taskId"].GetString(), nSize);
											bValidTask = true;
										}
									}
								}
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(taskMsg.szFactoryId, sizeof(taskMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
											bValidFactory = true;
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(taskMsg.szDeviceId, sizeof(taskMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
											bValidDevice = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(taskMsg.szOrg, sizeof(taskMsg.szOrg), doc["orgId"].GetString(), nSize);
											bValidOrg = true;
										}
									}
								}
								if (doc.HasMember("guarder")) {
									if (doc["guarder"].IsString()) {
										size_t nSize = doc["guarder"].GetStringLength();
										if (nSize) {
											strncpy_s(taskMsg.szGuarder, sizeof(taskMsg.szGuarder),
												doc["guarder"].GetString(), nSize);
											bValidGuarder = true;
										}
									}
								}
								if (doc.HasMember("taskType")) {
									if (doc["taskType"].IsInt()) {
										taskMsg.usTaskType = (unsigned short)doc["taskType"].GetInt();
										bValidType = true;
									}
								}
								if (doc.HasMember("limit")) {
									if (doc["limit"].IsInt()) {
										taskMsg.usTaskLimit = (unsigned short)doc["limit"].GetInt();
										bValidLimit = true;
									}
								}
								if (doc.HasMember("destination")) {
									if (doc["destination"].IsString()) {
										size_t nSize = doc["destination"].GetStringLength();
										if (nSize) {
											strncpy_s(taskMsg.szDestination, sizeof(taskMsg.szDestination),
												doc["destination"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("target")) {
									if (doc["target"].IsString()) {
										size_t nSize = doc["target"].GetStringLength();
										if (nSize) {
											strncpy_s(taskMsg.szTarget, sizeof(taskMsg.szTarget),
												doc["target"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											taskMsg.ulMessageTime = strdatetime2time(szDatetime);
											bValidDatetime = true;
										}
									}
								}
								if (bValidTask && bValidFactory && bValidDevice && bValidOrg && bValidGuarder
									&& bValidType && bValidLimit && bValidDatetime) {
									handleTopicTaskSubmitMsg(&taskMsg);
									storeTopicMsg(pMsg, taskMsg.ulMessageTime);
								}
								else {
									snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]submit task data miss, uuid=%s, seq=%u"
										", taskId=%s, factoryId=%s, deviceId=%s, orgId=%s, guarder=%s, type=%u, limit=%u, "
										"destination=%s, targer=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid,
										pMsg->usMsgSequence, bValidTask ? taskMsg.szTaskId : "null",
										bValidFactory ? taskMsg.szFactoryId : "null", bValidDevice ? taskMsg.szDeviceId : "null",
										bValidOrg ? taskMsg.szOrg : "null", bValidGuarder ? taskMsg.szGuarder : "null",
										taskMsg.usTaskType, taskMsg.usTaskLimit, taskMsg.szDestination, taskMsg.szTarget,
										bValidDatetime ? szDatetime : "null");
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
							}
							else if (nState == 1 || nState == 2) {
								TopicTaskCloseMessage taskCloseMsg;
								bool bValidTask = false;
								bool bValidDatetime = false;
								taskCloseMsg.nClose = nState;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strncpy_s(taskCloseMsg.szTaskId, sizeof(taskCloseMsg.szTaskId), 
												doc["taskId"].GetString(), nSize);
											bValidTask = true;
										}
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											taskCloseMsg.ulMessageTime = strdatetime2time(szDatetime);
											bValidDatetime = true;
										}
									}
								}
								if (bValidTask && bValidDatetime) {
									handleTopicTaskCloseMsg(&taskCloseMsg);
									storeTopicMsg(pMsg, taskCloseMsg.ulMessageTime);
								}
								else {
									snprintf(szLog, sizeof(szLog), "[DbProx]%s[%d]close task data miss, uuid=%s, seq=%u, "
										"task=%s, close=%d, stopdatetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid,
										pMsg->usMsgSequence, bValidTask ? taskCloseMsg.szTaskId : "null", nState,
										bValidDatetime ? szDatetime : "null");
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								}
							}
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_TASK data, unsupport task type=%d\r\n",
								__FUNCTION__, __LINE__, nState);
							writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_TASK msg data, uuid=%s, seq=%u, "
							"parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
					break;
				}
				case MSG_BUFFER_MODIFY: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						int nModifyObject = 0;
						int nModifyType = 0;
						bool bValidModifyType = false;
						bool bValidModifyObject = false;
						bool bValidDatetime = false;
						char szDateTime[20] = { 0 };
						if (doc.HasMember("object")) {
							if (doc["object"].IsInt()) {
								nModifyObject = doc["object"].GetInt();
								bValidModifyObject = true;
							}
						}
						if (doc.HasMember("operate")) {
							if (doc["operate"].IsInt()) {
								nModifyType = doc["operate"].GetInt();
								bValidModifyType = true;
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString()) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									strncpy_s(szDateTime, sizeof(szDateTime), doc["datetime"].GetString(), nSize);
									bValidDatetime = true;
								}
							}
						}
						if (bValidModifyObject && bValidModifyType && bValidDatetime) {
							switch (nModifyObject) {
								case BUFFER_DEVICE: {
									break;
								}
								case BUFFER_GUARDER: {
									break;
								}
								default: {
									snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_BUFFER_MODIFY msg data, "
										"uuid=%s, seq=%u, object=%d, operate=%d, datetime=%s, not support object\r\n",
										__FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence, nModifyObject,
										nModifyType, szDateTime);
									writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
									break;
								}
							}
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_BUFFER_MODIFY msg data, uuid=%s"
								", seq=%u, parameter miss, object=%d, operate=%d, datetime=%s\r\n", __FUNCTION__,
								__LINE__, pMsg->szMsgUuid, pMsg->usMsgSequence, bValidModifyObject ? nModifyObject : 0,
								bValidModifyType ? nModifyType : 0, bValidDatetime ? szDateTime : "");
							writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_BUFFER_MODIFY msg data, uuid=%s, "
							"seq=%u, parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid,
							pMsg->usMsgSequence);
						writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
					}
					break;
				}
				default: {
					snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]unsupport msg type: %u\r\n", __FUNCTION__,
						__LINE__, pMsg->usMsgType);
					writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
					break;
				}
			}
		}
	} while (1);
}

void DbProxy::storeTopicMsg(TopicMessage * pMsg_, unsigned long ulMsgTime_)
{
	if (pMsg_) {
		char szLog[256] = { 0 };
		char szSql[512] = { 0 };
		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(ulMsgTime_, szSqlDatetime, sizeof(szSqlDatetime));
		snprintf(szSql, sizeof(szSql), "insert into message_info (msgUuid, msgType, msgSeq, msgTopic, "
			"msgBody, msgTime) values ('%s', %u, %u, '%s', '%s', '%s');", pMsg_->szMsgUuid, 
			pMsg_->usMsgType, pMsg_->usMsgSequence, pMsg_->szMsgMark, pMsg_->szMsgBody, szSqlDatetime);
		size_t nSize = sizeof(dbproxy::SqlTransaction);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nSize);
		if (pTransaction) {
			pTransaction->uiSqlCount = 1;
			pTransaction->uiTransactionSequence = getNextInteractSequence();
			pTransaction->ulTransactionTime = (unsigned long)time(NULL);
			pTransaction->szTransactionFrom[0] = '\0';
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_MESSAGE;
			size_t nSqlLen = strlen(szSql);
			pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
			pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
			strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
			pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
			if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
				for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
					if (pTransaction->pSqlList[i].pStatement) {
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
					}
				}
				free(pTransaction->pSqlList);
				pTransaction->pSqlList = NULL;
				free(pTransaction);
				pTransaction = NULL;
			}
		}
	}
}

int DbProxy::handleTopicAliveMsg(TopicAliveMessage * pAliveMsg_)
{
	int result = -1;
	if (pAliveMsg_) {
		bool bLastest = false;
		bool bUpdateState = false;
		pthread_mutex_lock(&g_mutex4DevList);
		    //pAliveMsg_->szDeviceId
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
				pAliveMsg_->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
					changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
					bUpdateState = true;
				}
				if (pDev->deviceBasic.ulLastActiveTime < pAliveMsg_->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pAliveMsg_->ulMessageTime;
					pDev->deviceBasic.nBattery = pAliveMsg_->usBattery;
					bLastest = true;
				}
				result = 0;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (bLastest) {
			char szSqlDatetime[20] = { 0 };
			format_sqldatetime(pAliveMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
			char szDevSql[512] = { 0 };
			if (bUpdateState) {
				snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastCommuncation='%s', "
					"Power=%u, Online=1 where DeviceID='%s';", szSqlDatetime, pAliveMsg_->usBattery,
					pAliveMsg_->szDeviceId);
			}
			else {
				snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastCommuncation='%s', "
					"Power=%u where DeviceID='%s';", szSqlDatetime, pAliveMsg_->usBattery,
					pAliveMsg_->szDeviceId);
			}
			dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
				sizeof(dbproxy::SqlTransaction));
			pTransaction->szTransactionFrom[0] = '\0';
			pTransaction->uiSqlCount = 1;
			pTransaction->uiTransactionSequence = getNextInteractSequence();
			pTransaction->ulTransactionTime = (unsigned long)time(NULL);
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nSqlLen = strlen(szDevSql);
			pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
			pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
			strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szDevSql, nSqlLen);
			pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
			if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
				for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
					if (pTransaction->pSqlList[i].pStatement) {
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
					}
				}
				free(pTransaction->pSqlList);
				pTransaction->pSqlList = NULL;
				free(pTransaction);
				pTransaction = NULL;
			}
		}
	}
	return result;
}

int DbProxy::handleTopicOnlineMsg(TopicOnlineMessage * pOnlineMsg_)
{
	int result = -1;
	if (pOnlineMsg_) {
		bool bLastest = false;
		pthread_mutex_lock(&g_mutex4DevList);
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList,
				pOnlineMsg_->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nStatus == DEV_OFFLINE) {
					changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
				}
				if (pDev->deviceBasic.ulLastActiveTime < pOnlineMsg_->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pOnlineMsg_->ulMessageTime;
					pDev->deviceBasic.nBattery = pOnlineMsg_->usBattery;
					bLastest = true;
				}
				result = 0;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (bLastest) {
			char szSqlDatetime[20] = { 0 };
			format_sqldatetime(pOnlineMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
			unsigned long ulTime = (unsigned long)time(NULL);
			char szSqlNow[20] = { 0 };
			format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
			char szSql[512] = { 0 };
			snprintf(szSql, sizeof(szSql), "update device_info set LastCommuncation='%s', Power=%u,"
				" Online=1, LastOptTime='%s' where DeviceID='%s';", szSqlDatetime, pOnlineMsg_->usBattery,
				szSqlNow, pOnlineMsg_->szDeviceId);
			dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
				sizeof(dbproxy::SqlTransaction));
			pTransaction->szTransactionFrom[0] = '\0';
			pTransaction->uiSqlCount = 1;
			pTransaction->uiTransactionSequence = getNextInteractSequence();
			pTransaction->ulTransactionTime = ulTime;
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nSqlLen = strlen(szSql);
			pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
			pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
			strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
			pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
			if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
				for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
					if (pTransaction->pSqlList[i].pStatement) {
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
					}
				}
				free(pTransaction->pSqlList);
				pTransaction->pSqlList = NULL;
				free(pTransaction);
				pTransaction = NULL;
			}
		}
	}
	return result;
}

int DbProxy::handleTopicOfflineMsg(TopicOfflineMessage * pOfflineMsg_)
{
	int result = -1;
	bool bLastest = false;
	if (pOfflineMsg_) {
		pthread_mutex_lock(&g_mutex4DevList);
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
				pOfflineMsg_->szDeviceId);
			if (pDev) {
				result = 0;
				if (pDev->deviceBasic.nStatus != DEV_OFFLINE) {
					pDev->deviceBasic.nStatus = DEV_OFFLINE;
					bLastest = true;
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (bLastest) {
			unsigned long ulTime = (unsigned long)time(NULL);
			char szSqlNow[20] = { 0 };
			format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
			char szDevSql[512] = { 0 };
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set Online=0, Power=0, LastOptTime='%s'"
				" where DeviceID='%s';", szSqlNow, pOfflineMsg_->szDeviceId);
			dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
				sizeof(dbproxy::SqlTransaction));
			pTransaction->uiSqlCount = 1;
			pTransaction->szTransactionFrom[0] = '\0';
			pTransaction->uiTransactionSequence = getNextInteractSequence();
			pTransaction->ulTransactionTime = ulTime;
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nSqlLen = strlen(szDevSql);
			pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
			pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
			strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szDevSql, nSqlLen);
			pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
			if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
				for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
					if (pTransaction->pSqlList[i].pStatement) {
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
					}
				}
				free(pTransaction->pSqlList);
				pTransaction->pSqlList = NULL;
				free(pTransaction);
				pTransaction = NULL;
			}
		}
	}
	return result;
}

int DbProxy::handleTopicBindMsg(TopicBindMessage * pBindMsg_)
{
	int result = -1;
	if (pBindMsg_) {
		pthread_mutex_lock(&g_mutex4DevList);
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
				pBindMsg_->szDeviceId);
			if (pDev) {
				result = 0;
				strncpy_s(pDev->szBindGuard, sizeof(pDev->szBindGuard), pBindMsg_->szGuarder, 
					strlen(pBindMsg_->szGuarder));
				pDev->ulBindTime = pBindMsg_->ulMessageTime;
				pDev->deviceBasic.ulLastActiveTime = pBindMsg_->ulMessageTime;
				pDev->deviceBasic.nBattery = pBindMsg_->usBattery;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pBindMsg_->szGuarder);
		if (pGuarder) {
			//snprintf(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), "%s_%s",
			//	pBindMsg_->szFactoryId, pBindMsg_->szDeviceId);
			strncpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), pBindMsg_->szDeviceId, 
				strlen(pBindMsg_->szDeviceId));
			pGuarder->uiState = STATE_GUARDER_BIND;
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		
		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pBindMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		char szSqlNow[20] = { 0 };
		format_sqldatetime((unsigned long)time(NULL), szSqlNow, sizeof(szSqlNow));
		char szDevSql[512] = { 0 };
		snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastCommuncation='%s', Power=%u"
			" , LastOptTime='%s' where DeviceID='%s';", szSqlDatetime, pBindMsg_->usBattery,
			szSqlNow, pBindMsg_->szDeviceId);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
			sizeof(dbproxy::SqlTransaction));
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiSqlCount = 1;
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = (unsigned long)time(NULL);
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
			* sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
		size_t nSqlLen = strlen(szDevSql);
		pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szDevSql, nSqlLen);
		pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
		if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
			for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
				if (pTransaction->pSqlList[i].pStatement) {
					free(pTransaction->pSqlList[i].pStatement);
					pTransaction->pSqlList[i].pStatement = NULL;
				}
			}
			free(pTransaction->pSqlList);
			pTransaction->pSqlList = NULL;
			free(pTransaction);
			pTransaction = NULL;
		}
	}
	return result;
}

int DbProxy::handleTopicTaskSubmitMsg(TopicTaskMessage * pTaskMsg_)
{
	int result = -1;
	if (pTaskMsg_) {
		EscortTask * pTask = (EscortTask *)zmalloc(sizeof(EscortTask));
		if (pTask) {
			result = 0;
			strncpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), pTaskMsg_->szTaskId, 
				strlen(pTaskMsg_->szTaskId));
			strncpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), pTaskMsg_->szFactoryId,
				strlen(pTaskMsg_->szFactoryId));
			strncpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), pTaskMsg_->szDeviceId,
				strlen(pTaskMsg_->szDeviceId));
			strncpy_s(pTask->szOrg, sizeof(pTask->szOrg), pTaskMsg_->szOrg, strlen(pTaskMsg_->szOrg));
			strncpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), pTaskMsg_->szGuarder,
				strlen(pTaskMsg_->szGuarder));
			strncpy_s(pTask->szTarget, sizeof(pTask->szTarget), pTaskMsg_->szTarget,
				strlen(pTaskMsg_->szTarget));
			strncpy_s(pTask->szDestination, sizeof(pTask->szDestination), pTaskMsg_->szDestination,
				strlen(pTaskMsg_->szDestination));
			pTask->nTaskState = 0;
			pTask->nTaskFlee = 0;
			pTask->nTaskLimitDistance = (uint8_t)pTaskMsg_->usTaskLimit;
			pTask->nTaskType = (uint8_t)pTaskMsg_->usTaskType;
			format_datetime(pTaskMsg_->ulMessageTime, pTask->szTaskStartTime, 
				sizeof(pTask->szTaskStartTime));
			pTask->szTaskStopTime[0] = '\0';
			pthread_mutex_lock(&g_mutex4TaskList);
			zhash_update(g_taskList, pTaskMsg_->szTaskId, pTask);
			zhash_freefn(g_taskList, pTaskMsg_->szTaskId, free);
			pthread_mutex_unlock(&g_mutex4TaskList);
		}

		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pTaskMsg_->szGuarder);
		if (pGuarder) {
			pGuarder->uiState = STATE_GUARDER_DUTY;
			strncpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), pTaskMsg_->szTaskId,
				strlen(pTaskMsg_->szTaskId));
			format_datetime(pTaskMsg_->ulMessageTime, pGuarder->szTaskStartTime, 
				sizeof(pGuarder->szTaskStartTime));
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);

		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pTaskMsg_->szDeviceId);
		if (pDev) {
			pDev->deviceBasic.ulLastActiveTime = pTaskMsg_->ulMessageTime;
			changeDeviceStatus(DEV_GUARD, pDev->deviceBasic.nStatus);
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		
		char szPersonId[20] = { 0 };
		Person person;
		bool bNewPerson = false;
		if (makePerson(pTaskMsg_->szTarget, &person)) {
			strncpy_s(szPersonId, sizeof(szPersonId), person.szPersonId, strlen(person.szPersonId));
			pthread_mutex_lock(&g_mutex4PersonList);
			Person * pPerson = (Person *)zhash_lookup(g_personList, person.szPersonId);
			if (!pPerson) {
				pPerson = (Person *)zmalloc(sizeof(Person));
				memcpy_s(pPerson, sizeof(Person), &person, sizeof(Person));
				pPerson->nFlee = 1;
				bNewPerson = true;
				zhash_update(g_personList, szPersonId, pPerson);
				zhash_freefn(g_personList, szPersonId, free);
			}
			else {
				pPerson->nFlee = 1;
			}
			pthread_mutex_unlock(&g_mutex4PersonList);
		}

		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pTaskMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		char szSqlNow[20] = { 0 };
		format_sqldatetime((unsigned long)time(NULL), szSqlNow, sizeof(szSqlNow));
		char szTaskSql[512] = { 0 };
		char szDevSql[512] = { 0 };
		char szPersonSql[512] = { 0 };
		snprintf(szTaskSql, sizeof(szTaskSql), "insert into task_info (TaskID, TaskType, LimitDistance"
			", StartTime, Destination, UserID, TaskState, PersonID, DeviceID) values ('%s', %u, %u, '%s'"
			", '%s', '%s', 0, '%s', '%s');", pTask->szTaskId, pTask->nTaskType, pTask->nTaskLimitDistance,
			szSqlDatetime, pTask->szDestination, pTask->szGuarder, szPersonId, pTask->szDeviceId);
		snprintf(szDevSql, sizeof(szDevSql), "update device_info set IsUse=1, LastCommuncation='%s', "
			"LastOptTime='%s' where DeviceID='%s';", szSqlDatetime, szSqlNow, pTaskMsg_->szDeviceId);
		if (bNewPerson) {
			snprintf(szPersonSql, sizeof(szPersonSql), "insert into person_info (PersonID, PersonName, "
				"IsEscorting) values ('%s', '%s', 1);", person.szPersonId, person.szPersonName);
		}
		else {
			snprintf(szPersonSql, sizeof(szPersonSql), "update person_info set IsEscorting=1 where "
				"PersonID='%s';", person.szPersonId);
		}
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
			sizeof(dbproxy::SqlTransaction));
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = (unsigned long)time(NULL);
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiSqlCount = 3;
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
			* sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK;
		size_t nTaskSqlLen = strlen(szTaskSql);
		pTransaction->pSqlList[0].ulStatementLen = nTaskSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nTaskSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nTaskSqlLen + 1, szTaskSql, nTaskSqlLen);
		pTransaction->pSqlList[0].pStatement[nTaskSqlLen] = '\0';
		pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
		size_t nDevSqlLen = strlen(szDevSql);
		pTransaction->pSqlList[1].ulStatementLen = nDevSqlLen;
		pTransaction->pSqlList[1].pStatement = (char *)zmalloc(nDevSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[1].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
		pTransaction->pSqlList[1].pStatement[nDevSqlLen] = '\0';
		pTransaction->pSqlList[2].uiCorrelativeTable = escort_db::E_TBL_PERSON;
		size_t nPersonSqlLen = strlen(szPersonSql);
		pTransaction->pSqlList[2].ulStatementLen = nPersonSqlLen;
		pTransaction->pSqlList[2].pStatement = (char *)zmalloc(nPersonSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[2].pStatement, nPersonSqlLen + 1, szPersonSql, nPersonSqlLen);
		pTransaction->pSqlList[2].pStatement[nPersonSqlLen] = '\0';
		if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
			for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
				if (pTransaction->pSqlList[i].pStatement) {
					free(pTransaction->pSqlList[i].pStatement);
					pTransaction->pSqlList[i].pStatement = NULL;
					pTransaction->pSqlList[i].ulStatementLen = 0;
				}
			}
			free(pTransaction->pSqlList);
			pTransaction->pSqlList = NULL;
			free(pTransaction);
			pTransaction = NULL;
		}
		result = 0;
	}
	return result;
}

int DbProxy::handleTopicTaskCloseMsg(TopicTaskCloseMessage * pCloseTaskMsg_)
{
	int result = -1;
	if (pCloseTaskMsg_) {
		char szDeviceId[16] = { 0 };
		char szGuarder[20] = { 0 };
		Person person;
		pthread_mutex_lock(&g_mutex4TaskList);
		EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, pCloseTaskMsg_->szTaskId);
		if (pTask) {
			strncpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId, strlen(pTask->szDeviceId));
			strncpy_s(szGuarder, sizeof(szGuarder), pTask->szGuarder, strlen(pTask->szGuarder));
			makePerson(pTask->szTarget, &person);
			format_datetime(pCloseTaskMsg_->ulMessageTime, pTask->szTaskStopTime,
				strlen(pTask->szTaskStopTime));
		}
		zhash_delete(g_taskList, pCloseTaskMsg_->szTaskId);
		pthread_mutex_unlock(&g_mutex4TaskList);

		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
		if (pDev) {
			changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
			pDev->deviceBasic.ulLastActiveTime = pCloseTaskMsg_->ulMessageTime;
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
		if (pGuarder) {
			pGuarder->uiState = STATE_GUARDER_BIND;
			pGuarder->szTaskId[0] = '\0';
			pGuarder->szTaskStartTime[0] = '\0';
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);

		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pCloseTaskMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		char szSqlNow[20] = { 0 };
		format_sqldatetime((unsigned long)time(NULL), szSqlNow, sizeof(szSqlNow));
		char szTaskSql[512] = { 0 };
		snprintf(szTaskSql, sizeof(szTaskSql), "update task_info set EndTime='%s', TaskState=%d "
			"where TaskID='%s';", szSqlDatetime, pCloseTaskMsg_->nClose, pCloseTaskMsg_->szTaskId);
		char szDevSql[512] = { 0 };
		snprintf(szDevSql, sizeof(szDevSql), "update device_info set IsUse=0, LastCommuncation='%s',"
			" LastOptTime='%s' where DeviceID='%s';", szSqlDatetime, szSqlNow, szDeviceId);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
			sizeof(dbproxy::SqlTransaction));
		char szPersonSql[256] = { 0 };
		if (strlen(person.szPersonId)) {
			pthread_mutex_lock(&g_mutex4PersonList);
			Person * pPerson = (Person *)zhash_lookup(g_personList, person.szPersonId);
			if (pPerson) {
				pPerson->nFlee = 0;
			}
			pthread_mutex_unlock(&g_mutex4PersonList);
			snprintf(szPersonSql, sizeof(szPersonSql), "update person_info set IsEscorting=0 where "
				"PersonID='%s';", person.szPersonId);
		}

		pTransaction->uiSqlCount = 3;
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = (unsigned long)time(NULL);
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
			* sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK;
		size_t nTaskSqlLen = strlen(szTaskSql);
		pTransaction->pSqlList[0].ulStatementLen = nTaskSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nTaskSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nTaskSqlLen + 1, szTaskSql, nTaskSqlLen);
		pTransaction->pSqlList[0].pStatement[nTaskSqlLen] = '\0';
		pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
		size_t nDevSqlLen = strlen(szDevSql);
		pTransaction->pSqlList[1].ulStatementLen = nDevSqlLen;
		pTransaction->pSqlList[1].pStatement = (char *)zmalloc(nDevSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[1].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
		pTransaction->pSqlList[1].pStatement[nDevSqlLen] = '\0';
		size_t nPersonLen = strlen(szPersonSql);
		pTransaction->pSqlList[2].ulStatementLen = nPersonLen;
		pTransaction->pSqlList[2].pStatement = (char *)zmalloc(nPersonLen + 1);
		strncpy_s(pTransaction->pSqlList[2].pStatement, nPersonLen + 1, szPersonSql, nPersonLen);
		pTransaction->pSqlList[2].pStatement[nPersonLen] = '\0';
		pTransaction->pSqlList[2].uiCorrelativeTable = escort_db::E_TBL_PERSON;

		if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
			for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
				if (pTransaction->pSqlList[i].pStatement) {
					free(pTransaction->pSqlList[i].pStatement);
					pTransaction->pSqlList[i].pStatement = NULL;
					pTransaction->pSqlList[i].ulStatementLen = 0;
				}
			}
			free(pTransaction->pSqlList);
			pTransaction->pSqlList = NULL;
			free(pTransaction);
			pTransaction = NULL;
		}
		result = 0;
	}
	return result;
}

int DbProxy::handleTopicGpsLocateMsg(TopicLocateMessageGps * pGpsLocateMsg_)
{
	int result = -1;
	if (pGpsLocateMsg_) {
		bool bLastest = false;
		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
			pGpsLocateMsg_->szDeviceId);
		char szTaskId[12] = { 0 };
		char szGuarder[20] = { 0 };
		bool bWork = false;
		bool bFlee = false;
		if (pDev) {
			bool bGuard = (pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD;
			bFlee = (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE;
			bWork = (bGuard || bFlee);
			if (bWork && strlen(pDev->szBindGuard)) {
				strncpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard, strlen(pDev->szBindGuard));
			}
			if (pGpsLocateMsg_->usFlag == 1) { //realtime
				if (pDev->ulLastLocateTime <= pGpsLocateMsg_->ulMessageTime) {
					pDev->ulLastLocateTime = pGpsLocateMsg_->ulMessageTime;
					pDev->nLastLocateType = LOCATE_GPS;
					pDev->deviceBasic.nBattery = pGpsLocateMsg_->usBattery;
					pDev->devicePosition.dLatitude = pGpsLocateMsg_->dLat;
					pDev->devicePosition.dLngitude = pGpsLocateMsg_->dLng;
					pDev->devicePosition.usLatType = pGpsLocateMsg_->usLatType;
					pDev->devicePosition.usLngType = pGpsLocateMsg_->usLngType;
					bLastest = true;
				}
			}		
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (bWork && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strncpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId, strlen(pGuarder->szTaskId));
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		result = 0;
		time_t nLocateTime = (time_t)pGpsLocateMsg_->ulMessageTime;
		struct tm tm_locateTime;
		localtime_s(&tm_locateTime, &nLocateTime);
		char szLocateDbName[32] = { 0 };
		snprintf(szLocateDbName, sizeof(szLocateDbName), "data%04d%02d.location_%02d", 
			tm_locateTime.tm_year + 1900, tm_locateTime.tm_mon + 1, tm_locateTime.tm_mday);
		char szSqlDatetime[20] = { 0 };
		snprintf(szSqlDatetime, sizeof(szSqlDatetime), "%04d-%02d-%02d %02d:%02d:%02d",
			tm_locateTime.tm_year + 1900, tm_locateTime.tm_mon + 1, tm_locateTime.tm_mday,
			tm_locateTime.tm_hour, tm_locateTime.tm_min, tm_locateTime.tm_sec);
		char szLocateSql[256] = { 0 };
		snprintf(szLocateSql, sizeof(szLocateSql), "insert into %s (LocationType, DeviceID, TaskID, "
			"IsOut, RecordTime, Latitude, Longitude, Speed, Course, Power) values (%d, '%s', '%s', %d, "
			"'%s', %.06f, %.06f, %.04f, %d, %u);", szLocateDbName, escort_db::E_LOCATE_GPS,
			pGpsLocateMsg_->szDeviceId, bWork ? szTaskId : "", (bWork && bFlee) ? 1 : 0, szSqlDatetime, 
			pGpsLocateMsg_->dLat, pGpsLocateMsg_->dLng, pGpsLocateMsg_->dSpeed, 
			pGpsLocateMsg_->nDirection, pGpsLocateMsg_->usBattery);
		char szLocateDbName2[32] = { 0 };
		snprintf(szLocateDbName2, sizeof(szLocateDbName2), "escort_locate.location_%02d",
			tm_locateTime.tm_mday);
		char szLocateSql2[256] = { 0 };
		snprintf(szLocateSql2, sizeof(szLocateSql2), "insert into %s (LocationType, DeviceID, TaskID, "
			"IsOut, RecordTime, Latitude, Longitude, Speed, Course, Power) values (%d, '%s', '%s', %d, "
			"'%s', %.06f, %.06f, %.04f, %d, %u);", szLocateDbName2, escort_db::E_LOCATE_GPS,
			pGpsLocateMsg_->szDeviceId, bWork ? szTaskId : "", (bWork && bFlee) ? 1 : 0, szSqlDatetime,
			pGpsLocateMsg_->dLat, pGpsLocateMsg_->dLng, pGpsLocateMsg_->dSpeed, 
			pGpsLocateMsg_->nDirection, pGpsLocateMsg_->usBattery);
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		unsigned long ulTime = (unsigned long)time(NULL);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
		pTransaction->uiSqlCount = 2;
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = ulTime;
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
			* sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		size_t nSqlLen = strlen(szLocateSql);
		pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szLocateSql, nSqlLen);
		pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
		pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		size_t nSqlLen2 = strlen(szLocateSql2);
		pTransaction->pSqlList[1].ulStatementLen = nSqlLen2;
		pTransaction->pSqlList[1].pStatement = (char *)zmalloc(nSqlLen2 + 1);
		strncpy_s(pTransaction->pSqlList[1].pStatement, nSqlLen2 + 1, szLocateSql2, nSqlLen2);
		pTransaction->pSqlList[1].pStatement[nSqlLen2] = '\0';

		if (!addSqlTransaction(pTransaction, SQLTYPE_OTHER)) {
			for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
				if (pTransaction->pSqlList[i].pStatement) {
					free(pTransaction->pSqlList[i].pStatement);
					pTransaction->pSqlList[i].pStatement = NULL;
				}
			}
			free(pTransaction->pSqlList);
			pTransaction->pSqlList = NULL;
			free(pTransaction);
			pTransaction = NULL;
		}
		if (bLastest) {
			char szSqlNow[20] = { 0 };
			format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
			char szDevSql[256] = { 0 };
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastLocation='%s', Latitude="
				"%.06f, Longitude=%.06f, LocationType=%d, Power=%u, Speed=%.04f, Coruse=%d, LastOptTime='%s'"
				" where DeviceID='%s';", szSqlDatetime, pGpsLocateMsg_->dLat, pGpsLocateMsg_->dLng,
				escort_db::E_LOCATE_GPS, pGpsLocateMsg_->usBattery, pGpsLocateMsg_->dSpeed,
				pGpsLocateMsg_->nDirection, szSqlNow, pGpsLocateMsg_->szDeviceId);
			dbproxy::SqlTransaction * pTransaction2 = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
			pTransaction2->szTransactionFrom[0] = '\0';
			pTransaction2->uiTransactionSequence = getNextInteractSequence();
			pTransaction2->ulTransactionTime = ulTime;
			pTransaction2->uiSqlCount = 1;
			pTransaction2->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction2->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction2->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nDevSqlLen = strlen(szDevSql);
			pTransaction2->pSqlList[0].ulStatementLen = nDevSqlLen;
			pTransaction2->pSqlList[0].pStatement = (char *)zmalloc(nDevSqlLen + 1);
			strncpy_s(pTransaction2->pSqlList[0].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
			pTransaction2->pSqlList[0].pStatement[nDevSqlLen] = '\0';
			if (!addSqlTransaction(pTransaction2, SQLTYPE_EXECUTE)) {
				for (unsigned int i = 0; i < pTransaction2->uiSqlCount; ++i) {
					if (pTransaction2->pSqlList[i].pStatement) {
						free(pTransaction2->pSqlList[i].pStatement);
						pTransaction2->pSqlList[i].pStatement = NULL;
					}
				}
				free(pTransaction2->pSqlList);
				pTransaction2->pSqlList = NULL;
				free(pTransaction2);
				pTransaction2 = NULL;
			}
		}
	}
	return result;
}

int DbProxy::handleTopicLbsLocateMsg(TopicLocateMessageLbs * pLbsLocateMsg_)
{
	int result = -1;
	if (pLbsLocateMsg_) {
		char szGuarder[20] = { 0 };
		char szTaskId[12] = { 0 };
		bool bWork = false;
		bool bFlee = false;
		bool bLastest = false;
		unsigned short usBattery = pLbsLocateMsg_->usBattery;

		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
			pLbsLocateMsg_->szDeviceId);
		if (pDev) {
			if (usBattery == 0) {
				usBattery = pDev->deviceBasic.nBattery;
			}
			bool bGuard = (pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD;
			bFlee = (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE;
			bWork = (bGuard || bFlee);
			if (bWork && strlen(pDev->szBindGuard)) {
				strncpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard, strlen(pDev->szBindGuard));
			}
			if (pLbsLocateMsg_->usFlag == 1) {
				if (pDev->ulLastLocateTime <= pLbsLocateMsg_->ulMessageTime) {
					pDev->nLastLocateType = LOCATE_LBS;
					pDev->ulLastLocateTime = pLbsLocateMsg_->ulMessageTime;
					pDev->devicePosition.dLatitude = pLbsLocateMsg_->dLat;
					pDev->devicePosition.dLngitude = pLbsLocateMsg_->dLng;
					pDev->devicePosition.usLatType = pLbsLocateMsg_->usLatType;
					pDev->devicePosition.usLngType = pLbsLocateMsg_->usLngType;
					pDev->devicePosition.nPrecision = pLbsLocateMsg_->nPrecision;
					bLastest = true;
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		if (bWork && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strncpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId, strlen(pGuarder->szTaskId));
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}

		time_t nLocateTime = (time_t)pLbsLocateMsg_->ulMessageTime;
		struct tm tm_locateTime;
		localtime_s(&tm_locateTime, &nLocateTime);

		char szLocateDbName[32] = { 0 };
		snprintf(szLocateDbName, sizeof(szLocateDbName), "data%04d%02d.location_%02d", 
			tm_locateTime.tm_year + 1900, tm_locateTime.tm_mon + 1, tm_locateTime.tm_mday);
		char szSqlDatetime[20] = { 0 };
		snprintf(szSqlDatetime, sizeof(szSqlDatetime), "%04d-%02d-%02d %02d:%02d:%02d",
			tm_locateTime.tm_year + 1900, tm_locateTime.tm_mon + 1, tm_locateTime.tm_mday,
			tm_locateTime.tm_hour, tm_locateTime.tm_min, tm_locateTime.tm_sec);
		char szLocateSql[256] = { 0 };
		snprintf(szLocateSql, sizeof(szLocateSql), "insert into %s (LocationType, DeviceID, TaskID, "
			"IsOut, RecordTime, Latitude, Longitude, Power) values (%d, '%s', '%s', %d, '%s', %.06f, "
			"%.06f, %u);", szLocateDbName, (int)escort_db::E_LOCATE_LBS, pLbsLocateMsg_->szDeviceId,
			bWork ? szTaskId : "", (bWork && bFlee) ? 1 : 0, szSqlDatetime, pLbsLocateMsg_->dLat,
			pLbsLocateMsg_->dLng, usBattery);
		char szLocateSql2[256] = { 0 };
		char szLocateDbName2[32] = { 0 };
		snprintf(szLocateDbName2, sizeof(szLocateDbName2), "escort_locate.location_%02d",
			tm_locateTime.tm_mday);
		snprintf(szLocateSql2, sizeof(szLocateSql2), "insert into %s (LocationType, DeviceID, TaskID,"
			" IsOut, RecordTime, Latitude, Longitude, Power) values (%d, '%s', '%s', %d, '%s', %.06f, "
			"%.06f, %u);", szLocateDbName2, (int)escort_db::E_LOCATE_LBS, pLbsLocateMsg_->szDeviceId,
			bWork ? szTaskId : "", (bWork && bFlee) ? 1 : 0, szSqlDatetime, pLbsLocateMsg_->dLat,
			pLbsLocateMsg_->dLng, usBattery);
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		unsigned long ulTime = (unsigned long)time(NULL);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
		pTransaction->uiSqlCount = 2;
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = ulTime;
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
			* sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		size_t nSqlLen = strlen(szLocateSql);
		pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szLocateSql, nSqlLen);
		pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
		pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		size_t nSqlLen2 = strlen(szLocateSql2);
		pTransaction->pSqlList[1].ulStatementLen = nSqlLen2;
		pTransaction->pSqlList[1].pStatement = (char *)zmalloc(nSqlLen2 + 1);
		strncpy_s(pTransaction->pSqlList[1].pStatement, nSqlLen2 + 1, szLocateSql2, nSqlLen2);
		pTransaction->pSqlList[1].pStatement[nSqlLen2] = '\0';
		if (!addSqlTransaction(pTransaction, SQLTYPE_OTHER)) {
			for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
				if (pTransaction->pSqlList[i].pStatement) {
					free(pTransaction->pSqlList[i].pStatement);
					pTransaction->pSqlList[i].pStatement = NULL;
				}
			}
			free(pTransaction->pSqlList);
			pTransaction->pSqlList = NULL;
			free(pTransaction);
			pTransaction = NULL;
		}
		if (bLastest) {
			char szSqlNow[20] = { 0 };
			format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
			char szDevSql[256] = { 0 };
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastLocation='%s', Latitude=%.06f"
				", Longitude=%.06f, LocationType=%d, Power=%u, Speed=0.0000, Coruse=0, LastOptTime='%s'"
				" where DeviceID='%s';", szSqlDatetime, pLbsLocateMsg_->dLat, pLbsLocateMsg_->dLng, 
				escort_db::E_LOCATE_LBS, usBattery, szSqlNow, pLbsLocateMsg_->szDeviceId);
			dbproxy::SqlTransaction * pTransaction2 = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
			pTransaction2->szTransactionFrom[0] = '\0';
			pTransaction2->uiTransactionSequence = getNextInteractSequence();
			pTransaction2->ulTransactionTime = ulTime;
			pTransaction2->uiSqlCount = 1;
			pTransaction2->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction2->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction2->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nDevSqlLen = strlen(szDevSql);
			pTransaction2->pSqlList[0].ulStatementLen = nDevSqlLen;
			pTransaction2->pSqlList[0].pStatement = (char *)zmalloc(nDevSqlLen + 1);
			strncpy_s(pTransaction2->pSqlList[0].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
			pTransaction2->pSqlList[0].pStatement[nDevSqlLen] = '\0';
			if (!addSqlTransaction(pTransaction2, SQLTYPE_EXECUTE)) {
				for (unsigned int i = 0; i < pTransaction2->uiSqlCount; ++i) {
					if (pTransaction2->pSqlList[i].pStatement) {
						free(pTransaction2->pSqlList[i].pStatement);
						pTransaction2->pSqlList[i].pStatement = NULL;
					}
				}
				free(pTransaction2->pSqlList);
				pTransaction2->pSqlList = NULL;
				free(pTransaction2);
				pTransaction2 = NULL;
			}
		}
		result = 0;
	}
	return result;
}

int DbProxy::handleTopicBtLocateMsg(TopicLocateMessagBt * pBtLocateMsg_)
{
	int result = -1;
	if (pBtLocateMsg_) {
		bool bLastest = false;
		char szGuarder[20] = { 0 };
		char szTaskId[12] = { 0 };
		bool bFlee = false;
		bool bWork = false;
		unsigned short usBattery = pBtLocateMsg_->usBattery;

		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
			pBtLocateMsg_->szDeviceId);
		if (pDev) {
			bool bGuard = (pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD;
			bFlee = (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE;
			bWork = (bGuard || bFlee);
			if (usBattery == 0) {
				usBattery = pDev->deviceBasic.nBattery;
			}
			if (bWork && strlen(pDev->szBindGuard)) {
				strncpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard, strlen(pDev->szBindGuard));
			}
			if (pDev->ulLastLocateTime < pBtLocateMsg_->ulMessageTime) {
				pDev->ulLastLocateTime = pBtLocateMsg_->ulMessageTime;
				pDev->guardPosition.dLatitude = pBtLocateMsg_->dLat;
				pDev->guardPosition.dLngitude = pBtLocateMsg_->dLng;
				pDev->nLastLocateType = LOCATE_BT;
				bLastest = true;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		if (bWork && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strncpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId, strlen(pGuarder->szTaskId));
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}

		time_t nLocateTime = (time_t)pBtLocateMsg_->ulMessageTime;
		struct tm tm_locateTime;
		localtime_s(&tm_locateTime, &nLocateTime);
		char szLocateDbName[32] = { 0 };
		snprintf(szLocateDbName, sizeof(szLocateDbName), "data%04d%02d.location_%02d", 
			tm_locateTime.tm_year + 1900, tm_locateTime.tm_mon + 1, tm_locateTime.tm_mday);
		char szSqlDatetime[20] = { 0 };
		snprintf(szSqlDatetime, sizeof(szSqlDatetime), "%04d-%02d-%02d %02d:%02d:%02d",
			tm_locateTime.tm_year + 1900, tm_locateTime.tm_mon + 1, tm_locateTime.tm_mday,
			tm_locateTime.tm_hour, tm_locateTime.tm_min, tm_locateTime.tm_sec);
		char szLocateSql[256] = { 0 };
		snprintf(szLocateSql, sizeof(szLocateSql), "insert into %s (LocationType, DeviceID, TaskID, "
			"IsOut, RecordTime, Latitude, Longitude, Power) values (%d, '%s', '%s', %d, '%s', %.06f, "
			"%.06f, %d);", szLocateDbName, escort_db::E_LOCATE_APP, pBtLocateMsg_->szDeviceId,
			bWork ? szTaskId : "", (bWork && bFlee) ? 1 : 0, szSqlDatetime, pBtLocateMsg_->dLat,
			pBtLocateMsg_->dLng, usBattery);
		char szLocateDbName2[32] = { 0 };
		snprintf(szLocateDbName2, sizeof(szLocateDbName2), "escort_locate.location_%02d",
			tm_locateTime.tm_mday);
		char szLocateSql2[256] = { 0 };
		snprintf(szLocateSql2, sizeof(szLocateSql2), "insert into %s (LocationType, DeviceID, TaskID,"
			" IsOut, RecordTime, Latitude, Longitude, Power) values (%d, '%s', '%s', %d, '%s', %.06f, "
			"%.06f, %d);", szLocateDbName2, escort_db::E_LOCATE_APP, pBtLocateMsg_->szDeviceId,
			bWork ? szTaskId : "", (bWork && bFlee) ? 1 : 0, szSqlDatetime, pBtLocateMsg_->dLat,
			pBtLocateMsg_->dLng, usBattery);
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		unsigned long ulTime = (unsigned long)time(NULL);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
		pTransaction->uiSqlCount = 2;
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = ulTime;
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
			* sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		size_t nSqlLen = strlen(szLocateSql);
		pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szLocateSql, nSqlLen);
		pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
		size_t nSqlLen2 = strlen(szLocateSql2);
		pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		pTransaction->pSqlList[1].ulStatementLen = nSqlLen2;
		pTransaction->pSqlList[1].pStatement = (char *)zmalloc(nSqlLen2 + 1);
		strncpy_s(pTransaction->pSqlList[1].pStatement, nSqlLen2 + 1, szLocateSql2, nSqlLen2);
		pTransaction->pSqlList[1].pStatement[nSqlLen2] = '\0';
		if (!addSqlTransaction(pTransaction, SQLTYPE_OTHER)) {
			for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
				if (pTransaction->pSqlList[i].pStatement) {
					free(pTransaction->pSqlList[i].pStatement);
					pTransaction->pSqlList[i].pStatement = NULL;
				}
			}
			free(pTransaction->pSqlList);
			pTransaction->pSqlList = NULL;
			free(pTransaction);
			pTransaction = NULL;
		}

		if (bLastest) {
			char szSqlNow[20] = { 0 };
			format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
			char szDevSql[256] = { 0 };
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastLocation='%s', Latitude="
				"%.06f, Longitude=%.06f, LocationType=%d, Power=%u, Speed=0.0000, Coruse=0, LastOptTime='%s'"
				" where DeviceID='%s';", szSqlDatetime, pBtLocateMsg_->dLat, pBtLocateMsg_->dLng, 
				escort_db::E_LOCATE_APP, usBattery, szSqlNow, pBtLocateMsg_->szDeviceId);
			dbproxy::SqlTransaction * pTransaction2 = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
			pTransaction2->szTransactionFrom[0] = '\0';
			pTransaction2->uiTransactionSequence = getNextInteractSequence();
			pTransaction2->ulTransactionTime = ulTime;
			pTransaction2->uiSqlCount = 1;
			pTransaction2->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction2->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction2->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nDevSqlLen = strlen(szDevSql);
			pTransaction2->pSqlList[0].ulStatementLen = nDevSqlLen;
			pTransaction2->pSqlList[0].pStatement = (char *)zmalloc(nDevSqlLen + 1);
			strncpy_s(pTransaction2->pSqlList[0].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
			pTransaction2->pSqlList[0].pStatement[nDevSqlLen] = '\0';
			if (!addSqlTransaction(pTransaction2, SQLTYPE_EXECUTE)) {
				for (unsigned int i = 0; i < pTransaction2->uiSqlCount; ++i) {
					if (pTransaction2->pSqlList[i].pStatement) {
						free(pTransaction2->pSqlList[i].pStatement);
						pTransaction2->pSqlList[i].pStatement = NULL;
					}
				}
				free(pTransaction2->pSqlList);
				pTransaction2->pSqlList = NULL;
				free(pTransaction2);
				pTransaction2 = NULL;
			}
		}
		result = 0;
	}
	return result;
}

int DbProxy::handleTopicLowpoweAlarmMsg(TopicAlarmMessageLowpower * pLowpoweAlarmMsg_)
{
	int result = -1;
	if (pLowpoweAlarmMsg_) {
		bool bWork = false;
		bool bLastest = false;
		char szGuarder[20] = { 0 };
		char szTaskId[12] = { 0 };
		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList,
			pLowpoweAlarmMsg_->szDeviceId);
		if (pDev) {
			bool bLowpower = (pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER;
			if (pDev->deviceBasic.ulLastActiveTime < pLowpoweAlarmMsg_->ulMessageTime) {
				pDev->deviceBasic.ulLastActiveTime = pLowpoweAlarmMsg_->ulMessageTime;
			}
			if (pDev->ulLastLowPowerAlertTime < pLowpoweAlarmMsg_->ulMessageTime) {
				pDev->ulLastLowPowerAlertTime = pLowpoweAlarmMsg_->ulMessageTime;
				pDev->deviceBasic.nBattery = pLowpoweAlarmMsg_->usBattery;
				if (pLowpoweAlarmMsg_->usMode == 0) {
					if (!bLowpower) {
						changeDeviceStatus(DEV_LOWPOWER, pDev->deviceBasic.nStatus);
						bLastest = true;
					}
				}
				else {
					if (bLowpower) {
						changeDeviceStatus(DEV_LOWPOWER, pDev->deviceBasic.nStatus, 1);
						bLastest = true;
					}
				}
			}	
			bWork = ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD)
				|| ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE);
			if (bWork && strlen(pDev->szBindGuard)) {
				strncpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard, strlen(pDev->szBindGuard));
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		if (bWork && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strncpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId, strlen(pGuarder->szTaskId));
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
	
		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pLowpoweAlarmMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		unsigned long ulTime = (unsigned long)time(NULL);
		char szSqlNow[20] = { 0 };
		format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
		char szDevSql[256] = { 0 };
		char szWarnSql[256] = { 0 };
		unsigned int uiCount = 0;
		if (bLastest) {	
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set Power=%u, LastCommuncation='%s',"
				" LastOptTime='%s' where DeviceID='%s';", pLowpoweAlarmMsg_->usBattery, szSqlDatetime,
				szSqlNow,	pLowpoweAlarmMsg_->szDeviceId);
			uiCount += 1;
		}
		if (bWork && strlen(szTaskId)) {
			snprintf(szWarnSql, sizeof(szWarnSql), "insert into alarm_info (TaskID, AlarmType, ActionType"
				", RecordTime) values ('%s', %d, %d, '%s');", szTaskId, escort_db::E_ALMTYPE_LOWPOWER,
				pLowpoweAlarmMsg_->usMode, szSqlDatetime);
			uiCount += 1;
		}
		if (uiCount) {
			dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
				sizeof(dbproxy::SqlTransaction));
			if (pTransaction) {
				pTransaction->uiSqlCount = uiCount;
				pTransaction->uiTransactionSequence = getNextInteractSequence();
				pTransaction->ulTransactionTime = ulTime;
				pTransaction->szTransactionFrom[0] = '\0';
				pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(uiCount
					* sizeof(dbproxy::SqlStatement));
				int i = 0;
				size_t nDevSqlLen = strlen(szDevSql);
				size_t nWarnSqlLen = strlen(szWarnSql);
				if (nDevSqlLen) {
					pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
					pTransaction->pSqlList[i].ulStatementLen = nDevSqlLen;
					pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nDevSqlLen + 1);
					strncpy_s(pTransaction->pSqlList[i].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
					pTransaction->pSqlList[i].pStatement[nDevSqlLen] = '\0';
					i++;
				}
				if (nWarnSqlLen) {
					pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_ALARM;
					pTransaction->pSqlList[i].ulStatementLen = nWarnSqlLen;
					pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nWarnSqlLen + 1);
					strncpy_s(pTransaction->pSqlList[i].pStatement, nWarnSqlLen + 1, szWarnSql, nWarnSqlLen);
					pTransaction->pSqlList[i].pStatement[nWarnSqlLen] = '\0';
					i++;
				}
				if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
					for (unsigned int i = 0; i < uiCount; i++) {
						if (pTransaction->pSqlList[i].pStatement) {
							free(pTransaction->pSqlList[i].pStatement);
							pTransaction->pSqlList[i].pStatement = NULL;
						}
					}
					free(pTransaction->pSqlList);
					pTransaction->pSqlList = NULL;
					free(pTransaction);
					pTransaction = NULL;
				}
			}
		}
		result = 0;
	}
	return result;
}

int DbProxy::handleTopicLooseAlarmMsg(TopicAlarmMessageLoose * pLooseAlarmMsg_)
{
	int result = -1;
	if (pLooseAlarmMsg_) {
		bool bWork = false;
		bool bLastest = false;
		char szGuarder[20] = { 0 };
		char szTaskId[12] = { 0 };

		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
			pLooseAlarmMsg_->szDeviceId);
		if (pDev) {
			if (pDev->deviceBasic.ulLastActiveTime < pLooseAlarmMsg_->ulMessageTime) {
				pDev->deviceBasic.ulLastActiveTime = pLooseAlarmMsg_->ulMessageTime;
			}
			bool bLoose = (pDev->deviceBasic.nStatus & DEV_LOOSE) == DEV_LOOSE; 
			bWork = ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD)
				|| ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE);
			if (bWork && strlen(pDev->szBindGuard)) {
				strncpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard, strlen(pDev->szBindGuard));
			}
			if (pDev->ulLastLooseAlertTime < pLooseAlarmMsg_->ulMessageTime) {
				pDev->ulLastLooseAlertTime = pLooseAlarmMsg_->ulMessageTime;
				pDev->deviceBasic.nBattery = pLooseAlarmMsg_->usBattery;
				bLastest = true;
				if (pLooseAlarmMsg_->usMode == 0) {
					if (!bLoose) {
						changeDeviceStatus(DEV_LOOSE, pDev->deviceBasic.nStatus);
						bLastest = true;
					}
				}
				else {
					if (bLoose) {
						changeDeviceStatus(DEV_LOOSE, pDev->deviceBasic.nStatus, 1);
						bLastest = true;
					}
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		if (bWork && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strncpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId, strlen(pGuarder->szTaskId));
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}

		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pLooseAlarmMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		unsigned long ulTime = (unsigned long)time(NULL);
		char szSqlNow[20] = { 0 };
		format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
		char szDevSql[256] = { 0 };
		char szWarnSql[256] = { 0 };
		unsigned int uiCount = 0;
		if (bLastest) {
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set Power=%u, LastCommuncation='%s', "
				"IsRemove=%u, LastOptTime='%s' where DeviceID='%s';", pLooseAlarmMsg_->usBattery, 
				szSqlDatetime, (pLooseAlarmMsg_->usMode == 0) ? 1 : 0, szSqlNow, pLooseAlarmMsg_->szDeviceId);
			uiCount += 1;
		}
		if (bWork && strlen(szTaskId)) {
			snprintf(szWarnSql, sizeof(szWarnSql), "insert into alarm_info (TaskID, AlarmType, ActionType, "
				"RecordTime) values ('%s', %u, %u, '%s');", szTaskId, escort_db::E_ALMTYPE_LOOSE,
				pLooseAlarmMsg_->usMode, szSqlNow);
			uiCount += 1;
		}
		if (uiCount) {
			dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
				sizeof(dbproxy::SqlTransaction));
			if (pTransaction) {
				pTransaction->uiSqlCount = uiCount;
				pTransaction->szTransactionFrom[0] = '\0';
				pTransaction->uiTransactionSequence = getNextInteractSequence();
				pTransaction->ulTransactionTime = ulTime;
				pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(
					uiCount * sizeof(dbproxy::SqlStatement));
				size_t nDevSqlLen = strlen(szDevSql);
				size_t nWarnSqlLen = strlen(szWarnSql);
				unsigned int i = 0;
				if (nDevSqlLen) {
					pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
					pTransaction->pSqlList[i].ulStatementLen = nDevSqlLen;
					pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nDevSqlLen + 1);
					strncpy_s(pTransaction->pSqlList[i].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
					pTransaction->pSqlList[i].pStatement[nDevSqlLen] = '\0';
					i++;
				}
				if (nWarnSqlLen) {
					pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_ALARM;
					pTransaction->pSqlList[i].ulStatementLen = nWarnSqlLen;
					pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nWarnSqlLen + 1);
					strncpy_s(pTransaction->pSqlList[i].pStatement, nWarnSqlLen + 1, szWarnSql, nWarnSqlLen);
					pTransaction->pSqlList[i].pStatement[nWarnSqlLen] = '\0';
					i++;
				}
				if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
					for (unsigned int i = 0; i < uiCount; i++) {
						if (pTransaction->pSqlList[i].pStatement) {
							free(pTransaction->pSqlList[i].pStatement);
							pTransaction->pSqlList[i].pStatement = NULL;
						}
					}
					free(pTransaction->pSqlList);
					pTransaction->pSqlList = NULL;
					free(pTransaction);
					pTransaction = NULL;
				}
			}
		}
		result = 0;
	}
	return result;
}

int DbProxy::handleTopicFleeAlarmMsg(TopicAlarmMessageFlee * pFleeAlarmMsg_)
{
	int result = -1;
	if (pFleeAlarmMsg_) {
		char szGuarder[20] = { 0 };
		char szTaskId[12] = { 0 };
		bool bLastest = false;
		bool bWork = false;
		
		pthread_mutex_lock(&g_mutex4TaskList);
		if (zhash_size(g_taskList) > 0) {
			EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, pFleeAlarmMsg_->szTaskId);
			if (pTask) {
				if (pFleeAlarmMsg_->usMode == 0) {
					pTask->nTaskFlee = 1;
				}
				else {
					pTask->nTaskFlee = 0;
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4TaskList);

		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
			pFleeAlarmMsg_->szDeviceId);
		if (pDev) {
			bool bGuard = (pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD;
			bool bFlee = (pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE;
			if (pDev->ulLastFleeAlertTime < pFleeAlarmMsg_->ulMessageTime) {
				pDev->ulLastFleeAlertTime = pFleeAlarmMsg_->ulMessageTime;
				pDev->deviceBasic.nBattery = pFleeAlarmMsg_->usBattery;
				if (pFleeAlarmMsg_->usMode == 0) {
					if (bGuard) {
						changeDeviceStatus(DEV_FLEE, pDev->deviceBasic.nStatus);
						bLastest = true;
					}
				}
				else {
					if (bFlee) {
						changeDeviceStatus(DEV_GUARD, pDev->deviceBasic.nStatus);
						bLastest = true;
					}
				}
			}
			bWork = (bGuard || bFlee);
			if (bWork && strlen(pDev->szBindGuard)) {
				strncpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard, strlen(pDev->szBindGuard));
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		if (bWork && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->uiState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strncpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId, strlen(pGuarder->szTaskId));
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}

		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pFleeAlarmMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		char szWarnSql[256] = { 0 };
		char szDevSql[256] = { 0 };
		char szTaskSql[256] = { 0 };
		unsigned int uiCount = 0;
		unsigned int i = 0;
		unsigned long ulTime = (unsigned long)time(NULL);
		char szSqlNow[20] = { 0 };
		format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));

		if (bLastest) {
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastCommuncation='%s', Power=%u"
				", LastOptTime='%s' where DeviceID='%s';", szSqlDatetime, pFleeAlarmMsg_->usBattery,
				szSqlNow, pFleeAlarmMsg_->szDeviceId);
			uiCount += 1;
			if (strlen(szTaskId)) {
				snprintf(szTaskSql, sizeof(szTaskSql), "update task_info set IsOut=%u where TaskID='%s';",
					(pFleeAlarmMsg_->usMode == 0) ? 1 : 0, szTaskId);
				uiCount += 1;
			}
		}
		if (bWork && strlen(szTaskId)) {
			snprintf(szWarnSql, sizeof(szWarnSql), "insert into alarm_info (TaskID, AlarmType, ActionType"
				", RecordTime) values ('%s', %d, %d, '%s');", szTaskId, escort_db::E_ALMTYPE_FLEE, 
				pFleeAlarmMsg_->usMode, szSqlDatetime);
			uiCount += 1;
		}
		if (uiCount) {
			dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
				sizeof(dbproxy::SqlTransaction));
			pTransaction->uiSqlCount = uiCount;
			pTransaction->szTransactionFrom[0] = '\0';
			pTransaction->uiTransactionSequence = getNextInteractSequence();
			pTransaction->ulTransactionTime = (unsigned long)time(NULL);
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(uiCount
				* sizeof(dbproxy::SqlStatement));
			size_t nDevSqlLen = strlen(szDevSql);
			size_t nWarnSqlLen = strlen(szWarnSql);
			size_t nTaskSqlLen = strlen(szTaskSql);
			if (nDevSqlLen) {
				pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
				pTransaction->pSqlList[i].ulStatementLen = nDevSqlLen;
				pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nDevSqlLen + 1);
				strncpy_s(pTransaction->pSqlList[i].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
				pTransaction->pSqlList[i].pStatement[nDevSqlLen] = '\0';
				i++;
			}
			if (nTaskSqlLen) {
				pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_TASK;
				pTransaction->pSqlList[i].ulStatementLen = nTaskSqlLen;
				pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nTaskSqlLen + 1);
				strncpy_s(pTransaction->pSqlList[i].pStatement, nTaskSqlLen + 1, szTaskSql, nTaskSqlLen);
				pTransaction->pSqlList[i].pStatement[nTaskSqlLen] = '\0';
				i++;
			}
			if (nWarnSqlLen) {
				pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_ALARM;
				pTransaction->pSqlList[i].ulStatementLen = nWarnSqlLen;
				pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nWarnSqlLen + 1);
				strncpy_s(pTransaction->pSqlList[i].pStatement, nWarnSqlLen + 1, szWarnSql, nWarnSqlLen);
				pTransaction->pSqlList[i].pStatement[nWarnSqlLen] = '\0';
				i++;
			}
			if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
				for (unsigned int i = 0; i < uiCount; i++) {
					if (pTransaction->pSqlList[i].pStatement) {
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
					}
				}
				free(pTransaction->pSqlList);
				pTransaction->pSqlList = NULL;
				free(pTransaction);
				pTransaction = NULL;
			}
		}
	}
	return result;
}

bool DbProxy::addInteractMsg(InteractionMessage * pMsg_)
{
	bool result = false;
	if (pMsg_ && pMsg_->pMsgContents && pMsg_->uiContentCount && pMsg_->uiContentLens) {
		pthread_mutex_lock(&m_mutex4InteractMsgQueu);
		m_interactMsgQue.push(pMsg_);
		if (m_interactMsgQue.size() == 1) {
			pthread_cond_signal(&m_cond4InteractMsgQue);
		}
		result = true;
		pthread_mutex_unlock(&m_mutex4InteractMsgQueu);
	}
	return result;
}

void DbProxy::dealInteractMsg()
{
	char szLog[512] = { 0 };
	do {
		pthread_mutex_lock(&m_mutex4InteractMsgQueu);
		while (m_nRun && m_interactMsgQue.empty()) {
			pthread_cond_wait(&m_cond4InteractMsgQue, &m_mutex4InteractMsgQueu);
		}
		if (!m_nRun && m_interactMsgQue.empty()) {
			pthread_mutex_unlock(&m_mutex4InteractMsgQueu);
			break;
		}
		InteractionMessage * pMsg = m_interactMsgQue.front();
		m_interactMsgQue.pop();
		pthread_mutex_unlock(&m_mutex4InteractMsgQueu);
		if (pMsg) {
			for (unsigned int i = 0; i < pMsg->uiContentCount; i++) {
				rapidjson::Document doc;
				if (!doc.Parse(pMsg->pMsgContents[i]).HasParseError()) {
					bool bValidMsg = false;
					if (doc.HasMember("mark") && doc.HasMember("version")) {
						if (doc["mark"].IsString() && doc["version"].IsString()) {
							size_t nSize1 = doc["mark"].GetStringLength();
							size_t nSize2 = doc["version"].GetStringLength();
							if (nSize1 && nSize2) {
								if (strcmp(doc["mark"].GetString(), "EC") == 0) {
									bValidMsg = true;
								}
							}
						}
					}
					if (bValidMsg) {
						int nType = 0;
						int nSequence = 0;
						char szDatetime[20] = { 0 };
						bool bValidType = false;
						bool bValidSeq = false;
						bool bValidDatetime = false;
						if (doc.HasMember("type")) {
							if (doc["type"].IsInt()) {
								nType = doc["type"].GetInt();
								bValidType = true;
							}
						}
						if (doc.HasMember("sequence")) {
							if (doc["sequence"].IsInt()) {
								nSequence = doc["sequence"].GetInt();
								bValidSeq = true;
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString()) {
								size_t nSize = doc["datetime"].GetStringLength();
								if (nSize) {
									strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
									bValidDatetime = true;
								}
							}
						}
						switch (nType) {
							case MSG_SUB_ALIVE: {
								pthread_mutex_lock(&m_mutex4RemoteLink);
								if (m_remoteLink.nActive == 0) {
									m_remoteLink.nActive = 1;
								}
								m_remoteLink.ulLastActiveTime = (unsigned long)time(NULL);
								pthread_mutex_unlock(&m_mutex4RemoteLink);
								break;
							}
							case MSG_SUB_SNAPSHOT: {
								break;
							}
							case MSG_SUB_REQUEST: {
								break;
							}
							case MSG_SUB_REPORT: {
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]interaction message unsupport type"
									"=%d\r\n", __FUNCTION__, __LINE__, nType);
								writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]invalid interaction message, msg=%s\r\n",
							__FUNCTION__, __LINE__, pMsg->pMsgContents[i]);
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
				}
				else {
					snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse interaction message failed, JSON "
						"data parse error: msg=%s\r\n", __FUNCTION__, __LINE__, pMsg->pMsgContents[i]);
					writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
				}
			}
			free(pMsg->pMsgContents);
			pMsg->pMsgContents = NULL;
			free(pMsg->uiContentLens);
			pMsg->uiContentLens = NULL;
			free(pMsg);
			pMsg = NULL;
		}
	} while (1);
}

void DbProxy::handleReception(escort_db::SqlContainer * pContainer_, const char * pIdentity_)
{
	if (pContainer_ && pIdentity_ && strlen(pIdentity_)) {
		bool bHitBuffer = false;
		escort_db::SqlContainer replyContainer;
		replyContainer.uiSqlOptSeq = pContainer_->uiSqlOptSeq;
		replyContainer.ulSqlOptTime = pContainer_->ulSqlOptTime;
		replyContainer.usSqlOptTarget = pContainer_->usSqlOptTarget;
		replyContainer.usSqlOptType = pContainer_->usSqlOptType;
		replyContainer.szSqlOptKey[0] = '\0';
		size_t nContainerSize = sizeof(escort_db::SqlContainer);
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		switch (pContainer_->usSqlOptTarget) {
			case escort_db::E_TBL_DEVICE: {
				if (pContainer_->usSqlOptType == escort_db::E_OPT_QUERY) {			
					char szDeviceId[16] = { 0 };
					if (strlen(pContainer_->szSqlOptKey)) {
						strncpy_s(szDeviceId, sizeof(szDeviceId), pContainer_->szSqlOptKey, 
							strlen(pContainer_->szSqlOptKey));
						pthread_mutex_lock(&g_mutex4DevList);
						WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
						if (pDev) {
							replyContainer.uiResultCount = 1;
							replyContainer.uiResultLen = replyContainer.uiResultCount * sizeof(WristletDevice);
							replyContainer.pStoreResult = (unsigned char *)zmalloc(replyContainer.uiResultLen + 1);
							memcpy_s(replyContainer.pStoreResult, replyContainer.uiResultLen, pDev,
								sizeof(WristletDevice));
							replyContainer.pStoreResult[replyContainer.uiResultLen] = '\0';
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4DevList);	
		
						if (bHitBuffer) {
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							size_t nFrameDataLen = nContainerSize + replyContainer.uiResultLen;
							unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
							memcpy_s(pFrameData, nContainerSize, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, replyContainer.uiResultLen + 1,
								replyContainer.pStoreResult, replyContainer.uiResultLen);
							pFrameData[nFrameDataLen] = '\0';
							zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_body);
							zmsg_send(&msg_reply, m_reception);
							free(pFrameData);
							pFrameData = NULL;
							return;
						}
						else {
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
								nTransactionSize);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							pTransaction->uiSqlCount = 1;
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long)time(NULL);
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select DeviceID, FactoryID, OrgId, LastCommuncation,"
								" LastLocation, Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove "
								"from device_info where DeviceID='%s';", szDeviceId);
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
							pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (!addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
									if (pTransaction->pSqlList[i].pStatement) {
										free(pTransaction->pSqlList[i].pStatement);
										pTransaction->pSqlList[i].pStatement = NULL;
									}
								}
								free(pTransaction->pSqlList);
								pTransaction->pSqlList = NULL;
								free(pTransaction);
								pTransaction = NULL;
							}
							return;
						}				
					}
					else { //query all device
						pthread_mutex_lock(&g_mutex4DevList);
						size_t nCellCount = zhash_size(g_deviceList);
						size_t nCellSize = sizeof(WristletDevice);
						size_t nSize = nCellSize * nCellCount;
						if (nCellCount) {
							WristletDevice * pDevList = (WristletDevice *)zmalloc(nSize);
							WristletDevice * pCellDev = (WristletDevice *)zhash_first(g_deviceList);
							size_t i = 0;
							while (pCellDev) {
								memcpy_s(&pDevList[i], nCellSize, pCellDev, nCellSize);
								pCellDev = (WristletDevice *)zhash_next(g_deviceList);
								i++;
							}
							replyContainer.uiResultCount = nCellCount;
							replyContainer.uiResultLen = nSize;
							replyContainer.pStoreResult = (unsigned char *)zmalloc(nSize + 1);
							memcpy_s(replyContainer.pStoreResult, nSize, pDevList, nSize);
							replyContainer.pStoreResult[nSize] = '\0';
							size_t nContainerSize = sizeof(escort_db::SqlContainer);
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							size_t nFrameDataLen = nContainerSize + nSize;
							unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
							memcpy_s(pFrameData, nFrameDataLen, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, nFrameDataLen - nContainerSize + 1, 
								pDevList, nSize);
							pFrameData[nFrameDataLen] = '\0';
							zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_body);
							zmsg_send(&msg_reply, m_reception);
							free(pFrameData);
							pFrameData = NULL;
							free(pDevList);
							pDevList = NULL;
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4DevList);
					}
					if (bHitBuffer) {
						return;
					}
					else {
						dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
							nTransactionSize);
						strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
							pIdentity_, strlen(pIdentity_));
						pTransaction->uiSqlCount = 1;
						pTransaction->uiTransactionSequence = getNextInteractSequence();
						pTransaction->ulTransactionTime = (unsigned long)time(NULL);
						pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
							* sizeof(dbproxy::SqlStatement));
						char szSql[512] = { 0 };
						snprintf(szSql, sizeof(szSql), "select DeviceID, FactoryID, OrgId, LastCommuncation, "
							"LastLocation, Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove from "
							"device_info order by DeviceID, FactoryID;");
						size_t nSqlLen = strlen(szSql);
						pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
						pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
						pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
						memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
						pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
						if (!addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
							for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
								if (pTransaction->pSqlList[i].pStatement) {
									free(pTransaction->pSqlList[i].pStatement);
									pTransaction->pSqlList[i].pStatement = NULL;
								}
							}
							free(pTransaction->pSqlList);
							pTransaction->pSqlList = NULL;
							free(pTransaction);
							pTransaction = NULL;
						}
						return;
					}
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_DELETE) {
					char szDeviceId[20] = { 0 };
					strncpy_s(szDeviceId, sizeof(szDeviceId), pContainer_->szSqlOptKey, 
						strlen(pContainer_->szSqlOptKey));
					pthread_mutex_lock(&g_mutex4DevList);
					WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
					if (pDevice) {
						escort_db::SqlContainer replyContainer;
						replyContainer.uiResultCount = 1;
						replyContainer.uiSqlOptSeq = pContainer_->uiSqlOptSeq;
						replyContainer.ulSqlOptTime = pContainer_->ulSqlOptTime;
						replyContainer.usSqlOptTarget = pContainer_->usSqlOptTarget;
						replyContainer.usSqlOptType = pContainer_->usSqlOptType;
						size_t nDeviceSize = sizeof(WristletDevice);
						replyContainer.pStoreResult = (unsigned char *)zmalloc(nDeviceSize + 1);
						replyContainer.uiResultLen = nDeviceSize;
						memcpy_s(replyContainer.pStoreResult, nDeviceSize + 1, pDevice, nDeviceSize);
						replyContainer.pStoreResult[nDeviceSize] = '\0';
						size_t nContainerSize = sizeof(escort_db::SqlContainer);
						size_t nFrameDataLen = nContainerSize + replyContainer.uiResultLen;
						unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
						memcpy_s(pFrameData, nFrameDataLen, &replyContainer, nContainerSize);
						memcpy_s(pFrameData + nContainerSize, replyContainer.uiResultLen + 1,
							replyContainer.pStoreResult, replyContainer.uiResultLen);
						zmsg_t * msg_reply = zmsg_new();
						zframe_t * frame_identity = zframe_from(pIdentity_);
						zframe_t * frame_reply = zframe_new(pFrameData, nFrameDataLen);
						zmsg_append(msg_reply, &frame_identity);
						zmsg_append(msg_reply, &frame_reply);
						zmsg_send(&msg_reply, m_reception);
						free(pFrameData);
						pFrameData = NULL;
						free(replyContainer.pStoreResult);
						replyContainer.pStoreResult = NULL;
					}
					else {
						escort_db::SqlContainer replyContainer;
						replyContainer.uiResultCount = 0;
						replyContainer.uiResultLen = 0;
						replyContainer.pStoreResult = NULL;
						replyContainer.uiSqlOptSeq = pContainer_->uiSqlOptSeq;
						replyContainer.ulSqlOptTime = pContainer_->ulSqlOptTime;
						replyContainer.usSqlOptTarget = pContainer_->usSqlOptTarget;
						replyContainer.usSqlOptType = pContainer_->usSqlOptType;
						strncpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey),
							pContainer_->szSqlOptKey, strlen(pContainer_->szSqlOptKey));
						zmsg_t * msg_reply = zmsg_new();
						zframe_t * frame_identity = zframe_from(pIdentity_);
						zframe_t * frame_reply = zframe_new(&replyContainer, sizeof(escort_db::SqlContainer));
						zmsg_append(msg_reply, &frame_identity);
						zmsg_append(msg_reply, &frame_reply);
						zmsg_send(&msg_reply, m_reception);
					}
					zhash_delete(g_deviceList, szDeviceId);
					pthread_mutex_unlock(&g_mutex4DevList);
				}
				else if (pContainer_->usSqlOptTarget == escort_db::E_OPT_UPDATE) {

				}
				break;
			}
			case escort_db::E_TBL_GUARDER: {
				if (pContainer_->usSqlOptType == escort_db::E_OPT_QUERY) {
					char szGuarder[20] = { 0 };
					if (strlen(pContainer_->szSqlOptKey)) {
						strncpy_s(szGuarder, sizeof(szGuarder), pContainer_->szSqlOptKey, strlen(
							pContainer_->szSqlOptKey));
						pthread_mutex_lock(&g_mutex4GuarderList);
						Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
						if (pGuarder) {
							replyContainer.uiResultCount = 1;
							replyContainer.uiResultLen = replyContainer.uiResultCount * sizeof(Guarder);
							replyContainer.pStoreResult = (unsigned char *)zmalloc(replyContainer.uiResultLen + 1);
							memcpy_s(replyContainer.pStoreResult, replyContainer.uiResultLen, pGuarder,
								sizeof(Guarder));
							replyContainer.pStoreResult[replyContainer.uiResultLen] = '\0';
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4GuarderList);
						if (bHitBuffer) {
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							size_t nFrameDataLen = nContainerSize + replyContainer.uiResultLen;
							unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
							memcpy_s(pFrameData, nFrameDataLen, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, nFrameDataLen - nContainerSize + 1,
								replyContainer.pStoreResult, replyContainer.uiResultLen);
							pFrameData[nFrameDataLen] = '\0';
							zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_body);
							zmsg_send(&msg_reply, m_reception);
							free(pFrameData);
							pFrameData = NULL;
							return;
						}
						else {
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
								nTransactionSize);
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long)time(NULL);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							pTransaction->uiSqlCount = 1;
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							char szSql[256] = { 0 };
							snprintf(szSql, sizeof(szSql), "select UserID, UserName, Password, OrgID from "
								"user_info where UserID='%s' and (RoleType=2 or RoleType=3);", szGuarder);
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_GUARDER;
							pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (!addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
									if (pTransaction->pSqlList[i].pStatement) {
										free(pTransaction->pSqlList[i].pStatement);
										pTransaction->pSqlList[i].pStatement = NULL;
									}
								}
								free(pTransaction->pSqlList);
								pTransaction->pSqlList = NULL;
								free(pTransaction);
								pTransaction = NULL;
							}
							return;
						}
					}
					else { //query all guarder
						pthread_mutex_lock(&g_mutex4GuarderList);
						size_t nCellSize = sizeof(Guarder);
						size_t nCellCount = zhash_size(g_guarderList);
						size_t nSize = nCellSize * nCellCount;
						Guarder * pGuarderList = NULL;
						if (nCellCount) {
							pGuarderList = (Guarder *)zmalloc(nSize);
							Guarder * pGuarder = (Guarder *)zhash_first(g_guarderList);
							unsigned int i = 0;
							while (pGuarder) {
								memcpy_s(&pGuarderList[i], nCellSize, pGuarder, nCellSize);
								pGuarder = (Guarder *)zhash_next(g_guarderList);
								i++;
							}
							replyContainer.uiResultCount = nCellCount;
							replyContainer.uiResultLen = nSize;
							replyContainer.pStoreResult = (unsigned char *)zmalloc(nSize + 1);
							memcpy_s(replyContainer.pStoreResult, nSize, pGuarderList, nSize);
							replyContainer.pStoreResult[nSize] = '\0';
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4GuarderList);
						if (bHitBuffer) {
							if (pGuarderList) {
								zframe_t * frame_identity = zframe_from(pIdentity_);
								zframe_t * frame_empty = zframe_new(NULL, 0);
								size_t nContainerSize = sizeof(escort_db::SqlContainer);
								size_t nFrameDataLen = nContainerSize + nSize;
								unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
								memcpy_s(pFrameData, nFrameDataLen, &replyContainer, nContainerSize);
								memcpy_s(pFrameData + nContainerSize, nSize + 1, replyContainer.pStoreResult,
									nSize);
								pFrameData[nFrameDataLen] = '\0';
								zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
								zmsg_t * msg_reply = zmsg_new();
								zmsg_append(msg_reply, &frame_identity);
								zmsg_append(msg_reply, &frame_empty);
								zmsg_append(msg_reply, &frame_body);
								zmsg_send(&msg_reply, m_reception);
								free(pFrameData);
								pFrameData = NULL;
								return;
							}
						}
						else {
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
								sizeof(dbproxy::SqlTransaction));
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long)time(NULL);
							pTransaction->uiSqlCount = 1;
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_GUARDER;
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select UserID, UserName, Password, OrgID from user_"
								"info where (RoleType=2 or RoleType=3) order by UserID;");
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (!addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
									if (pTransaction->pSqlList[i].pStatement) {
										free(pTransaction->pSqlList[i].pStatement);
										pTransaction->pSqlList[i].pStatement = NULL;
									}
								}
								free(pTransaction->pSqlList);
								pTransaction->pSqlList = NULL;
								free(pTransaction);
								pTransaction = NULL;
							}
							return;
						}
					}
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_DELETE) {
					pthread_mutex_lock(&g_mutex4GuarderList);
					zhash_delete(g_guarderList, pContainer_->szSqlOptKey);
					pthread_mutex_unlock(&g_mutex4GuarderList);
					//reply
					escort_db::SqlContainer replyContainer;
					replyContainer.pStoreResult = NULL;
					replyContainer.uiResultCount = 0;
					replyContainer.uiResultLen = 0;
					strncpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey),
						pContainer_->szSqlOptKey, strlen(pContainer_->szSqlOptKey));
					replyContainer.uiSqlOptSeq = pContainer_->uiSqlOptSeq;
					replyContainer.ulSqlOptTime = pContainer_->ulSqlOptTime;
					replyContainer.usSqlOptTarget = pContainer_->usSqlOptTarget;
					replyContainer.usSqlOptType = pContainer_->usSqlOptType;
					zmsg_t * msg_reply = zmsg_new();
					zframe_t * frame_identity = zframe_from(pIdentity_);
					zframe_t * frame_empty = zframe_new(NULL, 0);
					zframe_t * frame_reply = zframe_new(&replyContainer, sizeof(replyContainer));
					zmsg_append(msg_reply, &frame_identity);
					zmsg_append(msg_reply, &frame_empty);
					zmsg_append(msg_reply, &frame_reply);
					zmsg_send(&msg_reply, m_reception);
				} 
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_UPDATE) {
					escort_db::SqlContainer replyContainer;
					replyContainer.pStoreResult = NULL;
					replyContainer.uiResultCount = 0;
					replyContainer.uiResultLen = 0;
					replyContainer.uiSqlOptSeq = pContainer_->uiSqlOptSeq;
					replyContainer.ulSqlOptTime = pContainer_->ulSqlOptTime;
					replyContainer.usSqlOptTarget = pContainer_->usSqlOptTarget;
					replyContainer.usSqlOptType = pContainer_->usSqlOptType;
					replyContainer.szSqlOptKey[0] = '\0';
					zmsg_t * msg_reply = zmsg_new();
					zframe_t * frame_identity = zframe_from(pIdentity_);
					zframe_t * frame_empty = zframe_new(NULL, 0);
					zframe_t * frame_reply = zframe_new(&replyContainer, nContainerSize);
					zmsg_append(msg_reply, &frame_identity);
					zmsg_append(msg_reply, &frame_empty);
					zmsg_append(msg_reply, &frame_reply);
					zmsg_send(&msg_reply, m_reception);
					if (strlen(pContainer_->szSqlOptKey) && pContainer_->uiResultLen && pContainer_->pStoreResult 
						&& pContainer_->uiResultCount == 1) { //update one data each
						char szGuarder[20] = { 0 };
						strncpy_s(szGuarder, sizeof(szGuarder), pContainer_->szSqlOptKey, 
							strlen(pContainer_->szSqlOptKey));
						size_t nGuarderSize = sizeof(Guarder);
						Guarder * pSrcGuarder = (Guarder *)zmalloc(nGuarderSize);
						memcpy_s(pSrcGuarder, nGuarderSize, pContainer_->pStoreResult, pContainer_->uiResultLen);
						bool bUpdateData = false;
						pthread_mutex_lock(&g_mutex4GuarderList);
						if (zhash_size(g_guarderList)) {
							Guarder * pDstGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
							if (pDstGuarder) {
								if (strcmp(pDstGuarder->szPassword, pSrcGuarder->szPassword) != 0) {
									strncpy_s(pDstGuarder->szPassword, sizeof(pDstGuarder->szPassword), pSrcGuarder->szPassword,
										strlen(pSrcGuarder->szPassword));
									bUpdateData = true;
								}
							}
						}
						pthread_mutex_unlock(&g_mutex4GuarderList);
						if (bUpdateData) {
							char szSqlDatetime[20] = { 0 };
							format_sqldatetime((unsigned long)time(NULL), szSqlDatetime, sizeof(szSqlDatetime));
							char szUpdateGuarderSql[256] = { 0 };
							snprintf(szUpdateGuarderSql, sizeof(szUpdateGuarderSql), "update user_info set Password='%s', "
								"LastOptTime='%s' where UserID='%s';", pSrcGuarder->szPassword, szSqlDatetime, szGuarder);
							size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
							dbproxy::SqlTransaction * pSqlTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
							pSqlTransaction->uiSqlCount = 1;
							pSqlTransaction->uiTransactionSequence = getNextInteractSequence();
							pSqlTransaction->ulTransactionTime = (unsigned long)time(NULL);
							pSqlTransaction->szTransactionFrom[0] = '\0';
							pSqlTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(sizeof(dbproxy::SqlStatement));
							pSqlTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_GUARDER;
							size_t nUpdateGuarderSqlLen = strlen(szUpdateGuarderSql);
							pSqlTransaction->pSqlList[0].ulStatementLen = nUpdateGuarderSqlLen;
							pSqlTransaction->pSqlList[0].pStatement = (char *)zmalloc(nUpdateGuarderSqlLen + 1);
							strncpy_s(pSqlTransaction->pSqlList[0].pStatement, nUpdateGuarderSqlLen + 1, szUpdateGuarderSql,
								nUpdateGuarderSqlLen);
							pSqlTransaction->pSqlList[0].pStatement[nUpdateGuarderSqlLen] = '\0';
							if (!addSqlTransaction(pSqlTransaction, SQLTYPE_EXECUTE)) {
								for (unsigned int i = 0; i < pSqlTransaction->uiSqlCount; i++) {
									if (pSqlTransaction->pSqlList[i].pStatement && pSqlTransaction->pSqlList[i].ulStatementLen) {
										free(pSqlTransaction->pSqlList[i].pStatement);
										pSqlTransaction->pSqlList[i].pStatement = NULL;
										pSqlTransaction->pSqlList[i].ulStatementLen = 0;
									}
								}
								free(pSqlTransaction->pSqlList);
								pSqlTransaction->pSqlList = NULL;
								free(pSqlTransaction);
								pSqlTransaction = NULL;
							}
						}
						free(pSrcGuarder);
						pSrcGuarder = NULL;
					}
				}
				break;
			}
			case escort_db::E_TBL_ORG: {
				if (pContainer_->usSqlOptType == escort_db::E_OPT_QUERY) {
					char szOrg[40] = { 0 };
					if (strlen(pContainer_->szSqlOptKey)) {
						strncpy_s(szOrg, sizeof(szOrg), pContainer_->szSqlOptKey, strlen(
							pContainer_->szSqlOptKey));
						pthread_mutex_lock(&g_mutex4OrgList);
						Organization * pOrg = (Organization *)zhash_lookup(g_orgList, szOrg);
						if (pOrg) {
							replyContainer.uiResultCount = 1;
							size_t nResultLen = replyContainer.uiResultCount * sizeof(Organization);
							replyContainer.uiResultLen = nResultLen;
							replyContainer.pStoreResult = (unsigned char *)zmalloc(nResultLen + 1);
							memcpy_s(replyContainer.pStoreResult, nResultLen, pOrg, nResultLen);
							replyContainer.pStoreResult[nResultLen] = '\0';
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4OrgList);
						if (bHitBuffer) {
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							size_t nFrameDataLen = nContainerSize + replyContainer.uiResultLen;
							unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
							memcpy_s(pFrameData, nFrameDataLen, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, nFrameDataLen - nContainerSize + 1,
								replyContainer.pStoreResult, replyContainer.uiResultLen);	
							pFrameData[nFrameDataLen] = '\0';
							zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_body);
							zmsg_send(&msg_reply, m_reception);
							free(pFrameData);
							pFrameData = NULL;
							return;
						}
						else {
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
								nTransactionSize);
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long)time(NULL);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							pTransaction->uiSqlCount = 1;
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select OrgID, OrgName, ParentID from org_info where "
								"OrgID='%s';", szOrg);
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_ORG;
							pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (!addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
									if (pTransaction->pSqlList[i].pStatement) {
										free(pTransaction->pSqlList[i].pStatement);
										pTransaction->pSqlList[i].pStatement = NULL;
									}
								}
								free(pTransaction->pSqlList);
								pTransaction->pSqlList = NULL;
								free(pTransaction);
								pTransaction = NULL;
							}
							return;
						}
					}
					else {
						Organization * pOrgList = NULL;
						pthread_mutex_lock(&g_mutex4OrgList);
						size_t nCellCount = zhash_size(g_orgList);
						size_t nCellSize = sizeof(Organization);
						size_t nSize = nCellCount * nCellSize;
						if (nCellCount) {
							pOrgList = (Organization *)zmalloc(nSize);
							Organization * pOrg = (Organization *)zhash_first(g_orgList);
							unsigned int i = 0;
							while (pOrg) {
								memcpy_s(&pOrgList[i], nCellSize, pOrg, nCellSize);
								pOrg = (Organization *)zhash_next(g_orgList);
								i++;
							}
							replyContainer.uiResultCount = nCellCount;
							replyContainer.uiResultLen = nSize;
							replyContainer.pStoreResult = (unsigned char *)zmalloc(nSize + 1);
							memcpy_s(replyContainer.pStoreResult, nSize, pOrgList, nSize);
							replyContainer.pStoreResult[nSize] = '\0';
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4OrgList);
						if (bHitBuffer) {
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							size_t nContainerSize = sizeof(escort_db::SqlContainer);
							size_t nFrameDataLen = nContainerSize + nSize;
							unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
							memcpy_s(pFrameData, nFrameDataLen, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, nSize + 1, replyContainer.pStoreResult,
								nSize);
							pFrameData[nContainerSize] = '\0';
							zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_body);
							zmsg_send(&msg_reply, m_reception);
							free(pFrameData);
							pFrameData = NULL;
							return;
						}
						else {
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
								sizeof(dbproxy::SqlTransaction));
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long)time(NULL);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							pTransaction->uiSqlCount = 1;
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select OrgID, OrgName, ParentID from org_info "
								"order by OrgID;");
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_ORG;
							pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (!addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
									if (pTransaction->pSqlList[i].pStatement) {
										free(pTransaction->pSqlList[i].pStatement);
										pTransaction->pSqlList[i].pStatement = NULL;
									}
								}
								free(pTransaction->pSqlList);
								pTransaction->pSqlList = NULL;
								free(pTransaction);
								pTransaction = NULL;
							}
							return;
						}
					}
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_DELETE) {
					pthread_mutex_lock(&g_mutex4OrgList);
					zhash_delete(g_orgList, pContainer_->szSqlOptKey);
					pthread_mutex_unlock(&g_mutex4OrgList);
					escort_db::SqlContainer replyContainer;
					replyContainer.pStoreResult = NULL;
					replyContainer.uiResultCount = 0;
					replyContainer.uiResultLen = 0;
					strncpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey),
						pContainer_->szSqlOptKey, strlen(pContainer_->szSqlOptKey));
					replyContainer.uiSqlOptSeq = pContainer_->uiSqlOptSeq;
					replyContainer.ulSqlOptTime = pContainer_->ulSqlOptTime;
					replyContainer.usSqlOptTarget = pContainer_->usSqlOptTarget;
					replyContainer.usSqlOptType = pContainer_->usSqlOptType;
					zmsg_t * msg_reply = zmsg_new();
					zframe_t * frame_identity = zframe_from(pIdentity_);
					zframe_t * frame_reply = zframe_new(&replyContainer, sizeof(replyContainer));
					zmsg_append(msg_reply, &frame_identity);
					zmsg_append(msg_reply, &frame_reply);
					zmsg_send(&msg_reply, m_reception);
				}
				break;
			}
			case escort_db::E_TBL_TASK: {
				if (pContainer_->usSqlOptType == escort_db::E_OPT_QUERY) {
					char szTaskId[12] = { 0 };
					if (strlen(pContainer_->szSqlOptKey)) {
						strncpy_s(szTaskId, sizeof(szTaskId), pContainer_->szSqlOptKey,
							strlen(pContainer_->szSqlOptKey));
						pthread_mutex_lock(&g_mutex4TaskList);
						EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szTaskId);
						if (pTask) {
							replyContainer.uiResultCount = 1;
							size_t nTaskLen = sizeof(EscortTask);
							size_t nResultLen = replyContainer.uiResultCount * nTaskLen;
							replyContainer.uiResultLen = nResultLen;
							replyContainer.pStoreResult = (unsigned char *)zmalloc(nResultLen + 1);
							memcpy_s(replyContainer.pStoreResult, nResultLen + 1, pTask, nTaskLen);
							replyContainer.pStoreResult[nResultLen] = '\0';
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4TaskList);
						if (bHitBuffer) {
							size_t nContainerSize = sizeof(escort_db::SqlContainer);
							size_t nFrameDataLen = nContainerSize + replyContainer.uiResultLen;
							unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
							memcpy_s(pFrameData, nFrameDataLen, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, nFrameDataLen - nContainerSize + 1,
								replyContainer.pStoreResult, replyContainer.uiResultLen);
							pFrameData[nFrameDataLen] = '\0';
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_body);
							zmsg_send(&msg_reply, m_reception);
							free(pFrameData);
							pFrameData = NULL;
						}
						else {
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
								sizeof(dbproxy::SqlTransaction));
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long)time(NULL);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select TaskID, TaskType, LimitDistance, StartTime, "
								"Destination, UserID as GuarderID, DeviceID, task_info.PersonID, person_info.Perso"
								"nName, IsOut from task_info, person_info where TaskState = 0 and task_info.Person"
								"ID = person_info.PersonID and TaskID='%s';", szTaskId);
							size_t nSqlLen = strlen(szSql);
							pTransaction->uiSqlCount = 1;
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK;
							pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (!addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
									if (pTransaction->pSqlList[i].pStatement) {
										free(pTransaction->pSqlList[i].pStatement);
										pTransaction->pSqlList[i].pStatement = NULL;
									}
								}
								free(pTransaction->pSqlList);
								pTransaction->pSqlList = NULL;
								free(pTransaction);
								pTransaction = NULL;
							}
						}
						return;
					}
					else { //query all task
						EscortTask * pTaskList = NULL;
						pthread_mutex_lock(&g_mutex4TaskList);
						size_t nCellSize = sizeof(EscortTask);
						size_t nCellCount = zhash_size(g_taskList);
						size_t nSize = nCellCount * nCellSize;
						if (nCellCount) {
							pTaskList = (EscortTask *)zmalloc(nSize);
							EscortTask * pTask = (EscortTask *)zhash_first(g_taskList);
							unsigned int i = 0;
							while (pTask) {
								memcpy_s(&pTaskList[i], nCellSize, pTask, nCellSize);
								pTask = (EscortTask *)zhash_next(g_taskList);
								i++;
							}
							replyContainer.uiResultCount = nCellCount;
							replyContainer.uiResultLen = nSize;
							replyContainer.pStoreResult = (unsigned char *)zmalloc(nSize + 1);
							memcpy_s(replyContainer.pStoreResult, nSize, pTaskList, nSize);
							replyContainer.pStoreResult[nSize] = '\0';
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4TaskList);
						if (bHitBuffer) {
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							size_t nContainerSize = sizeof(escort_db::SqlContainer);
							size_t nFrameDataLen = nContainerSize + nSize;
							unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
							memcpy_s(pFrameData, nFrameDataLen, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, nSize + 1, replyContainer.pStoreResult, nSize);
							pFrameData[nFrameDataLen] = '\0';
							zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_body);
							zmsg_send(&msg_reply, m_reception);
							free(pFrameData);
							pFrameData = NULL;
							return;
						}
						else {
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
								sizeof(dbproxy::SqlTransaction));
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long)time(NULL);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select TaskID, TaskType, LimitDistance, StartTime, "
								"Destination, UserID as GuarderID, DeviceID, task_info.PersonID, person_info.Perso"
								"nName, IsOut from task_info, person_info where TaskState = 0 and task_info.Person"
								"ID = person_info.PersonID order by TaskID; ");
							size_t nSqlLen = strlen(szSql);
							pTransaction->uiSqlCount = 1;
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK;
							pTransaction->pSqlList[0].ulStatementLen = nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (!addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
									if (pTransaction->pSqlList[i].pStatement) {
										free(pTransaction->pSqlList[i].pStatement);
										pTransaction->pSqlList[i].pStatement = NULL;
									}
								}
								free(pTransaction->pSqlList);
								pTransaction->pSqlList = NULL;
								free(pTransaction);
								pTransaction = NULL;
							}
							return;
						}
					}
				}
				break;
			}
			case escort_db::E_TBL_MESSAGE: {			
				break;
			}
			default: {
				break;
			}
		}
		replyContainer.uiResultCount = 0;
		replyContainer.uiResultLen = 0;
		replyContainer.pStoreResult = NULL;
		zframe_t * frame_identity = zframe_from(pIdentity_);
		zframe_t * frame_empty = zframe_new(NULL, 0);
		size_t nFrameDataLen = sizeof(replyContainer);
		unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
		memcpy_s(pFrameData, nFrameDataLen, &replyContainer, nFrameDataLen);
		pFrameData[nFrameDataLen] = '\0';
		zframe_t * frame_body = zframe_new(pFrameData, nFrameDataLen);
		zmsg_t * msg_reply = zmsg_new();
		zmsg_append(msg_reply, &frame_identity);
		zmsg_append(msg_reply, &frame_empty);
		zmsg_append(msg_reply, &frame_body);
		zmsg_send(&msg_reply, m_reception);
	}
}

bool DbProxy::makePerson(const char * pSrc_, Person * pDest_)
{
	bool result = false;
	if (pDest_) {
		memset(pDest_, 0, sizeof(Person));
	}
	if (pSrc_ && strlen(pSrc_)) {
		std::string str = pSrc_;
		size_t n = str.find_first_of('&');
		if (n != std::string::npos) {
			std::string foreStr = str.substr(0, n);
			std::string backStr = str.substr(n + 1);
			if (pDest_) {
				size_t nForeStrSize = foreStr.size();
				size_t nBackStrSize = backStr.size();
				if (nForeStrSize < sizeof(pDest_->szPersonId)) {
					strncpy_s(pDest_->szPersonId, sizeof(pDest_->szPersonId), foreStr.c_str(), nForeStrSize);
				}
				else {
					strncpy_s(pDest_->szPersonId, sizeof(pDest_->szPersonId), foreStr.c_str(),
						sizeof(pDest_->szPersonId));
				}
				if (nBackStrSize < sizeof(pDest_->szPersonName)) {
					strncpy_s(pDest_->szPersonName, sizeof(pDest_->szPersonName), backStr.c_str(), nBackStrSize);
				}
				else {
					strncpy_s(pDest_->szPersonName, sizeof(pDest_->szPersonName), backStr.c_str(),
						sizeof(pDest_->szPersonName));
				}
			}
		}
		else {
			if (pDest_) {
				size_t nStrSize = str.size();
				if (nStrSize < sizeof(pDest_->szPersonId)) {
					strncpy_s(pDest_->szPersonId, sizeof(pDest_->szPersonId), str.c_str(), nStrSize);
				}
				else {
					strncpy_s(pDest_->szPersonId, sizeof(pDest_->szPersonId), str.c_str(),
						sizeof(pDest_->szPersonId));
				}
			}
		}
		result = true;
	}
	return result;
}

void DbProxy::changeDeviceStatus(unsigned short usNewStatus_, unsigned short & usDeviceStatus_,
	int nMode_)
{
	if (usNewStatus_ == DEV_LOWPOWER || usNewStatus_ == DEV_LOOSE) {
		if (usDeviceStatus_ != DEV_OFFLINE) {
			if (nMode_ == 0) {
				usDeviceStatus_ += ((usDeviceStatus_ & usNewStatus_) == usNewStatus_) ? 0 : usNewStatus_;
			}
			else {
				usDeviceStatus_ -= ((usDeviceStatus_ & usNewStatus_) == usNewStatus_) ? usNewStatus_ : 0;
			}
		}
	}
	else if (usNewStatus_ == DEV_OFFLINE) {
		usDeviceStatus_ = DEV_OFFLINE;
	}
	else if (usNewStatus_ == DEV_ONLINE || usNewStatus_ == DEV_GUARD || usNewStatus_ == DEV_FLEE) {
		usDeviceStatus_ = usDeviceStatus_ 
			- (((usDeviceStatus_ & DEV_ONLINE) == DEV_ONLINE) ? DEV_ONLINE : 0)
			- (((usDeviceStatus_ & DEV_GUARD) == DEV_GUARD) ? DEV_GUARD : 0)
			- (((usDeviceStatus_ & DEV_FLEE) == DEV_FLEE) ? DEV_FLEE : 0)
			+ usNewStatus_;
	}
}

unsigned int DbProxy::getNextInteractSequence()
{
	unsigned int result = 0;
	pthread_mutex_lock(&g_mutex4InteractSequence);
	if (++g_uiInteractSequence == 0) {
		g_uiInteractSequence = 1;
	}
	result = g_uiInteractSequence;
	pthread_mutex_unlock(&g_mutex4InteractSequence);
	return result;
}

unsigned short DbProxy::getNextPipeSequence()
{
	unsigned short result = 0;
	pthread_mutex_lock(&g_mutex4PipeSequence);
	if (++g_usPipeSequence == 0) {
		g_usPipeSequence = 1;
	}
	result = g_usPipeSequence;
	pthread_mutex_unlock(&g_mutex4PipeSequence);
	return result;
}

void DbProxy::initZookeeper()
{
	int nTimeout = 30000;
	if (strlen(m_szZkHost)) {
		if (!m_zkHandle) {
			m_zkHandle = zookeeper_init(m_szZkHost, zk_server_watcher, nTimeout, NULL, this, 0);
		}
		if (m_zkHandle) {
			zoo_acreate(m_zkHandle, "/escort", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0, 
				zk_escort_create_completion, this);
			zoo_acreate(m_zkHandle, "/escort/dbproxy", "", 1024, &ZOO_OPEN_ACL_UNSAFE, 0,
				zk_dbproxy_create_completion, this);
		}
	}
}

int DbProxy::competeForMaster()
{
	if (m_bZKConnected) {
		char * path = make_zkpath(2, ESCORT_DBPROXY_PATH, "master");
		int ret = zoo_acreate(m_zkHandle, path, "", 1024, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL,
			zk_dbproxy_master_create_completion, this);
		free(path);
		return ret;
	}
	return -1;
}

void DbProxy::masterExist()
{
	if (m_bZKConnected) {
		char * path = make_zkpath(2, ESCORT_DBPROXY_PATH, "master");
		zoo_awexists(m_zkHandle, path, zk_dbproxy_master_exists_watcher, this,
			zk_dbproxy_master_exists_completion, this);
		free(path);
	}
}

int DbProxy::runAsSlaver()
{
	if (m_bZKConnected) {
		char * path = make_zkpath(2, ESCORT_DBPROXY_PATH, "slaver_");
		int ret = zoo_acreate(m_zkHandle, path, "", 1024, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL
			+ ZOO_SEQUENCE, zk_dbproxy_slaver_create_completion, this);
		free(path);
		return ret;
	}
	return -1;
}

void DbProxy::removeSlaver()
{
	if (m_bZKConnected) {
		zoo_delete(m_zkHandle, m_zkNodePath, -1);
		m_zkNodePath[0] = '\0';
	}
}

int DbProxy::setZkDbProxyData(const char * pPath_, void * pData_, size_t nDataSize_)
{
	if (m_bZKConnected) {
		char szBuf[1024] = { 0 };
		memcpy_s(szBuf, 1024, pData_, nDataSize_);
		int ret = zoo_aset(m_zkHandle, pPath_, szBuf, (int)nDataSize_, -1, zk_dbproxy_set_completion,
			this);
		return ret;
	}
	return -1;
}

int DbProxy::getZkMidwareData(const char * pPath_, ZkMidware * pData_)
{
	if (m_bZKConnected) {
		char * path = make_zkpath(2, ESCORT_MIDWARE_PATH, "master");
		char szBuf[1024] = { 0 };
		int nBufLen = 1024;
		Stat stat;
		int rc = zoo_get(m_zkHandle, path, 0, szBuf, &nBufLen, &stat);
		if (rc == ZOK) {
			memcpy_s(pData_, sizeof(ZkMidware), szBuf, sizeof(ZkMidware));
		}
		free(path);
		path = NULL;
	}
	return -1;
}

int DbProxy::sendDataViaInteractor(const char * pData_, size_t nDataLen_)
{
	int result = -1;
	if (pData_ && nDataLen_) {
		unsigned char * pFrameData = (unsigned char *)zmalloc(nDataLen_ + 1);
		memcpy_s(pFrameData, nDataLen_, pData_, nDataLen_);
		pFrameData[nDataLen_] = '\0';
		zmsg_t * msg = zmsg_new();
		zmsg_addmem(msg, pFrameData, nDataLen_);
		zmsg_send(&msg, m_interactor);
		free(pFrameData);
		pFrameData = NULL;
		result = 0;
	}
	return result;
}

bool DbProxy::initSqlBuffer()
{
	bool result = true;
	char szPersonSql[512] = { 0 };//1
	char szOrgSql[512] = { 0 }; //2
	char szDeviceSql[512] = { 0 };//3
	char szGuarderSql[512] = { 0 };//4
	char szTaskSql[512] = { 0 };//5
	snprintf(szPersonSql, sizeof(szPersonSql), "select PersonID, PersonName, IsEscorting from "
		"person_info order by PersonID;");
	snprintf(szOrgSql, sizeof(szOrgSql), "select OrgID, OrgName, ParentID from org_info order by "
		"OrgID;");
	snprintf(szDeviceSql, sizeof(szDeviceSql), "select DeviceID, FactoryID, OrgId, LastCommuncation, "
		"LastLocation, Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove from "
		"device_info order by DeviceID, FactoryID;");
	snprintf(szGuarderSql, sizeof(szGuarderSql), "select UserID, UserName, Password, OrgID from "
		"user_info where (RoleType = 2 or RoleType = 3) order by UserID;");	
	snprintf(szTaskSql, sizeof(szTaskSql), "select TaskID, TaskType, LimitDistance, StartTime, "
		"Destination, UserID as GuarderID, DeviceID, task_info.PersonID, person_info.PersonName, IsOut "
		"from task_info, person_info where TaskState = 0 and task_info.PersonID = person_info.PersonID "
		"order by TaskID;");
	size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
	dbproxy::SqlTransaction * pSqlTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
	pSqlTransaction->uiSqlCount = 5;
	pSqlTransaction->uiTransactionSequence = getNextInteractSequence();
	pSqlTransaction->szTransactionFrom[0] = '\0';
	pSqlTransaction->ulTransactionTime = (unsigned long)time(NULL);
	pSqlTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pSqlTransaction->uiSqlCount
		* sizeof(dbproxy::SqlStatement));
	size_t nPersonSqlLen = strlen(szPersonSql);
	pSqlTransaction->pSqlList[0].ulStatementLen = nPersonSqlLen;
	pSqlTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_PERSON;
	pSqlTransaction->pSqlList[0].pStatement = (char *)zmalloc(nPersonSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[0].pStatement, nPersonSqlLen + 1, szPersonSql, nPersonSqlLen);
	pSqlTransaction->pSqlList[0].pStatement[nPersonSqlLen] = '\0';
	
	size_t nOrgSqlLen = strlen(szOrgSql);
	pSqlTransaction->pSqlList[1].ulStatementLen = nOrgSqlLen;
	pSqlTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_ORG;
	pSqlTransaction->pSqlList[1].pStatement = (char *)zmalloc(nOrgSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[1].pStatement, nOrgSqlLen + 1, szOrgSql, nOrgSqlLen);
	pSqlTransaction->pSqlList[1].pStatement[nOrgSqlLen] = '\0';

	size_t nDeviceSqlLen = strlen(szDeviceSql);
	pSqlTransaction->pSqlList[2].ulStatementLen = nDeviceSqlLen;
	pSqlTransaction->pSqlList[2].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
	pSqlTransaction->pSqlList[2].pStatement = (char *)zmalloc(nDeviceSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[2].pStatement, nDeviceSqlLen + 1, szDeviceSql, nDeviceSqlLen);
	pSqlTransaction->pSqlList[2].pStatement[nDeviceSqlLen] = '\0';

	size_t nGuarderSqlLen = strlen(szGuarderSql);
	pSqlTransaction->pSqlList[3].ulStatementLen = nGuarderSqlLen;
	pSqlTransaction->pSqlList[3].uiCorrelativeTable = escort_db::E_TBL_GUARDER;
	pSqlTransaction->pSqlList[3].pStatement = (char *)zmalloc(nGuarderSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[3].pStatement, nGuarderSqlLen + 1, szGuarderSql, nGuarderSqlLen);
	pSqlTransaction->pSqlList[3].pStatement[nGuarderSqlLen] = '\0';

	size_t nTaskSqlLen = strlen(szTaskSql);
	pSqlTransaction->pSqlList[4].ulStatementLen = nTaskSqlLen;
	pSqlTransaction->pSqlList[4].uiCorrelativeTable = escort_db::E_TBL_TASK;
	pSqlTransaction->pSqlList[4].pStatement = (char *)zmalloc(nTaskSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[4].pStatement, nTaskSqlLen + 1, szTaskSql, nTaskSqlLen);
	pSqlTransaction->pSqlList[4].pStatement[nTaskSqlLen] = '\0';
	if (!addSqlTransaction(pSqlTransaction, SQLTYPE_QUERY)) {
		for (unsigned int i = 0; i < pSqlTransaction->uiSqlCount; i++) {
			free(pSqlTransaction->pSqlList[i].pStatement);
			pSqlTransaction->pSqlList[i].pStatement = NULL;
		}
		free(pSqlTransaction->pSqlList);
		pSqlTransaction->pSqlList = NULL;
		free(pSqlTransaction);
		pSqlTransaction = NULL;
		result = false;
	}
	return result; 
}

int supervise(zloop_t * loop_, int timer_id_, void * arg_)
{
	int result = 0;
	DbProxy * pProxy = (DbProxy *)arg_;
	if (pProxy) {
		if (!pProxy->m_nRun) {
			result = -1;
		}
		else {
			if (pProxy->m_nTimerTickCount % 6 == 0) { //1.0 min
				bool bActived = false;
				pthread_mutex_lock(&pProxy->m_mutex4RemoteLink);
				if (pProxy->m_remoteLink.nActive) {
					bActived = true;
				}
				if (bActived) {
					time_t currTime = time(NULL);
					time_t lastActiveTime = pProxy->m_remoteLink.ulLastActiveTime;
					double interval = difftime(currTime, lastActiveTime);
					if (interval > 180.00) { //3min
						char szDatetime[20] = { 0 };
						format_datetime((unsigned long)currTime, szDatetime, sizeof(szDatetime));
						char szMsg[256] = { 0 };
						snprintf(szMsg, sizeof(szMsg), "{\"mark\":\"EC\",\"version\":\"10\",\"type\":%d,\"sequence\
							\":%u,\"datetime\":\"%s\"}", MSG_SUB_ALIVE, pProxy->getNextInteractSequence(), szDatetime);
						if (strlen(szMsg)) {
							pProxy->sendDataViaInteractor(szMsg, strlen(szMsg));
						}
					}
					if (interval > 300.00) { //5min
						pProxy->m_remoteLink.nActive = 0;
					}
				}
				pthread_mutex_unlock(&pProxy->m_mutex4RemoteLink);
			}
			if (pProxy->m_nTimerTickCount % 12 == 0) { //2min
				time_t now = time(NULL);
				pthread_mutex_lock(&DbProxy::g_mutex4UpdateTime);
				double interval = difftime(now, (time_t)DbProxy::g_ulLastUpdateTime);
				if (interval >= 120.0000) {
					if (pProxy->g_nUpdatePipeState == dbproxy::E_PIPE_CLOSE) {
						size_t nTaskSize = sizeof(dbproxy::UpdatePipeTask);
						dbproxy::UpdatePipeTask * pTask = (dbproxy::UpdatePipeTask *)zmalloc(nTaskSize);
						pTask->ulUpdateTaskTime = (unsigned long)now;
						if (!pProxy->addUpdateTask(pTask)) {
							free(pTask);
							pTask = NULL;
						}
						else {
							pProxy->g_nUpdatePipeState = dbproxy::E_PIPE_OPEN;
						}
					}
				}
				pthread_mutex_unlock(&DbProxy::g_mutex4UpdateTime);
			}
			if (pProxy->m_nTimerTickCount % 90 == 0) { //15min
				//keep connection alive
				char szLog[512] = { 0 };
				if (pProxy->m_locateConn) {
					if (mysql_ping(pProxy->m_locateConn) != 0) {
						//connection exception
						mysql_close(pProxy->m_locateConn);
						pProxy->m_locateConn = NULL;
						//re-connection
						pProxy->m_locateConn = mysql_init(NULL);
						if (pProxy->m_locateConn && mysql_real_connect(pProxy->m_locateConn, pProxy->m_zkDbProxy.szDbHostIp, 
							pProxy->m_zkDbProxy.szDbUser, pProxy->m_zkDbProxy.szDbPasswd, pProxy->m_zkDbProxy.szLocateSample,
							3306, NULL, 0)) {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect db %s, ip=%s, user=%s at %ld\r\n", 
								__FUNCTION__, __LINE__,	pProxy->m_zkDbProxy.szLocateSample,	pProxy->m_zkDbProxy.szDbHostIp, 
								pProxy->m_zkDbProxy.szDbUser, (long)time(NULL));
							pProxy->writeLog(szLog, LOGCATEGORY_INFORMATION, pProxy->m_nLogType);
						}
					}
				}
				if (pProxy->m_readConn) {
					if (mysql_ping(pProxy->m_readConn) != 0) {
						mysql_close(pProxy->m_readConn);
						pProxy->m_readConn = NULL;
						//re-connection
						pProxy->m_readConn = mysql_init(NULL);
						if (pProxy->m_readConn && mysql_real_connect(pProxy->m_readConn, pProxy->m_zkDbProxy.szDbHostIp,
							pProxy->m_zkDbProxy.szDbUser, pProxy->m_zkDbProxy.szDbPasswd, pProxy->m_zkDbProxy.szMajorSample,
							3306, NULL, 0)) {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect db %s, ip=%s, user=%s at %ld\r\n",
								__FUNCTION__, __LINE__, pProxy->m_zkDbProxy.szMajorSample, pProxy->m_zkDbProxy.szDbHostIp,
								pProxy->m_zkDbProxy.szDbUser, (long)time(NULL));
							pProxy->writeLog(szLog, LOGCATEGORY_INFORMATION, pProxy->m_nLogType);
						}
					}
				}
				if (pProxy->m_writeConn) {
					if (mysql_ping(pProxy->m_writeConn) != 0) {
						mysql_close(pProxy->m_writeConn);
						pProxy->m_writeConn = NULL;
						//re-connection
						pProxy->m_writeConn = mysql_init(NULL);
						if (pProxy->m_writeConn && mysql_real_connect(pProxy->m_writeConn, pProxy->m_zkDbProxy.szDbHostIp,
							pProxy->m_zkDbProxy.szDbUser, pProxy->m_zkDbProxy.szDbPasswd, pProxy->m_zkDbProxy.szMajorSample,
							3306, NULL, 0)) {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect db %s, ip=%s, user=%s at %ld\r\n",
								__FUNCTION__, __LINE__, pProxy->m_zkDbProxy.szMajorSample, pProxy->m_zkDbProxy.szDbHostIp,
								pProxy->m_zkDbProxy.szDbUser, (long)time(NULL));
							pProxy->writeLog(szLog, LOGCATEGORY_INFORMATION, pProxy->m_nLogType);
						}
					}
				}
				if (pProxy->m_updateConn) {
					if (mysql_ping(pProxy->m_updateConn) != 0) {
						mysql_close(pProxy->m_updateConn);
						pProxy->m_updateConn = NULL;
						//re-connection
						pProxy->m_updateConn = mysql_init(NULL);
						if (pProxy->m_updateConn && mysql_real_connect(pProxy->m_updateConn, pProxy->m_zkDbProxy.szDbHostIp,
							pProxy->m_zkDbProxy.szDbUser, pProxy->m_zkDbProxy.szDbPasswd, pProxy->m_zkDbProxy.szMajorSample,
							3306, NULL, 0)) {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect db %s, ip=%s, user=%s at %ld\r\n",
								__FUNCTION__, __LINE__, pProxy->m_zkDbProxy.szMajorSample, pProxy->m_zkDbProxy.szDbHostIp,
								pProxy->m_zkDbProxy.szDbUser, (long)time(NULL));
							pProxy->writeLog(szLog, LOGCATEGORY_INFORMATION, pProxy->m_nLogType);
						}
					}
				}
			}
			pProxy->m_nTimerTickCount++;
			if (pProxy->m_nTimerTickCount == 720) {
				pProxy->m_nTimerTickCount = 0;
			}
		}
	}
	return result;
}

void DbProxy::updatePipeLoop()
{
	char szLog[512] = { 0 };
	do {
		pthread_mutex_lock(&m_mutex4UpdatePipe);
		while (m_nRun && m_updateTaskQue.empty()) {
			pthread_cond_wait(&m_cond4UpdatePipe, &m_mutex4UpdatePipe);
		}
		if (!m_nRun) {
			pthread_mutex_unlock(&m_mutex4UpdatePipe);
			break;
		}
		dbproxy::UpdatePipeTask * pTask =	m_updateTaskQue.front();
		m_updateTaskQue.pop();
		pthread_mutex_unlock(&m_mutex4UpdatePipe);
		if (pTask) {
			do {
				if (g_nUpdatePipeState == dbproxy::E_PIPE_OPEN) {
					char szLastUpdateTime[20];
					format_sqldatetime(g_ulLastUpdateTime, szLastUpdateTime, sizeof(szLastUpdateTime));

					MYSQL_RES * res_ptr;
					int nErr;
					char szOrgSql[256] = { 0 };
					snprintf(szOrgSql, sizeof(szOrgSql), "select OrgID, OrgName, ParentID from org_info where"
						" LastOptTime >'%s' order by OrgID;", szLastUpdateTime);
					unsigned long ulOrgSqlLen = strlen(szOrgSql);
					char szGuarderSql[256] = { 0 };
					snprintf(szGuarderSql, sizeof(szGuarderSql), "select UserID, UserName, Password, OrgID "
						"from user_info where (RoleType = 2 or RoleType = 3) and LastOptTime > '%s' order by "
						"UserID;", szLastUpdateTime);
					unsigned long ulGuarderSqlLen = strlen(szGuarderSql);
					char szDeviceSql[256] = { 0 };
					snprintf(szDeviceSql, sizeof(szDeviceSql), "select DeviceID, FactoryId, OrgId, LastCommun"
						"cation, LastLocation, Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove"
						" from device_info where LastOptTime > '%s' order by DeviceID, FactoryId;", 
						szLastUpdateTime);
					unsigned long ulDeviceSqlLen = strlen(szDeviceSql);

					nErr = mysql_real_query(m_updateConn, szOrgSql, ulOrgSqlLen);
					if (nErr == 0) {
						res_ptr = mysql_store_result(m_updateConn);
						if (res_ptr) {
							my_ulonglong nRowCount = mysql_num_rows(res_ptr);
							if (nRowCount > 0) {
								MYSQL_ROW row;
								size_t nCount = (size_t)nRowCount;
								size_t nOrgSize = sizeof(escort_db::SqlOrg);
								escort_db::SqlOrg * pOrgList = (escort_db::SqlOrg *)zmalloc(nCount * nOrgSize);
								size_t i = 0;
								while (row = mysql_fetch_row(res_ptr)) {
									strncpy_s(pOrgList[i].szOrgId, sizeof(pOrgList[i].szOrgId), row[0], strlen(row[0]));
									strncpy_s(pOrgList[i].szOrgName, sizeof(pOrgList[i].szOrgName), row[1],
										strlen(row[1]));
									strncpy_s(pOrgList[i].szParentOrgId, sizeof(pOrgList[i].szParentOrgId), row[2],
										strlen(row[2]));
									i++;
								}
								pthread_mutex_lock(&g_mutex4OrgList);
								for (i = 0; i < nCount; i++) {
									Organization * pOrg = (Organization *)zhash_lookup(g_orgList, pOrgList[i].szOrgId);
									if (pOrg) {
										if (strcmp(pOrg->szOrgName, pOrgList[i].szOrgName) != 0) {
											strncpy_s(pOrg->szOrgName, sizeof(pOrg->szOrgName), pOrgList[i].szOrgName,
												strlen(pOrgList[i].szOrgName));
										}
										if (strcmp(pOrg->szParentOrgId, pOrgList[i].szParentOrgId) != 0) {
											strncpy_s(pOrg->szParentOrgId, sizeof(pOrg->szParentOrgId),
												pOrgList[i].szParentOrgId, strlen(pOrgList[i].szParentOrgId));
										}
									}
									else {
										pOrg = (Organization *)zmalloc(sizeof(Organization));
										strncpy_s(pOrg->szOrgId, sizeof(pOrg->szOrgId), pOrgList[i].szOrgId,
											strlen(pOrgList[i].szOrgId));
										strncpy_s(pOrg->szOrgName, sizeof(pOrg->szOrgName), pOrgList[i].szOrgName,
											strlen(pOrgList[i].szOrgName));
										strncpy_s(pOrg->szParentOrgId, sizeof(pOrg->szParentOrgId),
											pOrgList[i].szParentOrgId, strlen(pOrgList[i].szParentOrgId));
										zhash_update(g_orgList, pOrg->szOrgId, pOrg);
										zhash_freefn(g_orgList, pOrg->szOrgId, free);
									}
								}
								pthread_mutex_unlock(&g_mutex4OrgList);
								free(pOrgList);
								pOrgList = NULL;
							}
							mysql_free_result(res_ptr);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute OrgList sql update Loop error=%d,"
							"%s\r\n", __FUNCTION__, __LINE__, mysql_errno(m_updateConn), mysql_error(m_updateConn));
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
						break;
					}
					nErr = mysql_real_query(m_updateConn, szGuarderSql, ulGuarderSqlLen);
					if (nErr == 0) {
						res_ptr = mysql_store_result(m_updateConn);
						if (res_ptr) {
							my_ulonglong nRowCount = mysql_num_rows(res_ptr);
							if (nRowCount > 0) {
								MYSQL_ROW row;
								size_t nCount = (size_t)nRowCount;
								size_t nSqlGuarderSize = sizeof(escort_db::SqlGuarder);
								escort_db::SqlGuarder * pGuarderList = (escort_db::SqlGuarder *)zmalloc(
									nCount * nSqlGuarderSize);
								size_t i = 0;
								while (row = mysql_fetch_row(res_ptr)) {
									strncpy_s(pGuarderList[i].szUserId, sizeof(pGuarderList[i].szUserId), row[0],
										strlen(row[0]));
									strncpy_s(pGuarderList[i].szUserName, sizeof(pGuarderList[i].szUserName),
										row[1], strlen(row[1]));
									strncpy_s(pGuarderList[i].szPasswd, sizeof(pGuarderList[i].szPasswd), row[2],
										strlen(row[2]));
									strncpy_s(pGuarderList[i].szOrgId, sizeof(pGuarderList[i].szOrgId), row[3],
										strlen(row[3]));
									i++;
								}
								BufferUpdate bufferUpdateInfo;
								bufferUpdateInfo.usUpdateObject = BUFFER_GUARDER;
								bufferUpdateInfo.usUpdateDataCount = (unsigned short)nCount;
								size_t nGuarderSize = sizeof(Guarder);
								bufferUpdateInfo.uiUpdateDataLen = nCount * nGuarderSize;
								bufferUpdateInfo.pUpdateData = (unsigned char *)zmalloc(
									bufferUpdateInfo.uiUpdateDataLen + 1);
								size_t nOffset = 0;
								pthread_mutex_lock(&g_mutex4GuarderList);
								for (i = 0; i < nCount; i++) {
									Guarder * pGuarder = (Guarder *)zhash_lookup(g_deviceList, pGuarderList[i].szUserId);
									if (pGuarder) {
										if (strcmp(pGuarderList[i].szUserName, pGuarder->szTagName) != 0) {
											strncpy_s(pGuarder->szTagName, sizeof(pGuarder->szTagName),
												pGuarderList[i].szUserName, strlen(pGuarderList[i].szUserName));
										}
										if (strcmp(pGuarderList[i].szPasswd, pGuarder->szPassword) != 0) {
											strncpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword),
												pGuarderList[i].szPasswd, strlen(pGuarderList[i].szPasswd));
										}
										if (strcmp(pGuarderList[i].szOrgId, pGuarder->szOrg) != 0) {
											strncpy_s(pGuarder->szOrg, sizeof(pGuarder->szOrg), pGuarderList[i].szOrgId,
												strlen(pGuarderList[i].szOrgId));
										}
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen
											- nOffset, pGuarder, nGuarderSize);
										nOffset += nGuarderSize;
									}
									else {
										pGuarder = (Guarder *)zmalloc(nGuarderSize);
										memset(pGuarder, 0, nGuarderSize);
										strncpy_s(pGuarder->szId, sizeof(pGuarder->szId), pGuarderList[i].szUserId,
											strlen(pGuarderList[i].szUserId));
										strncpy_s(pGuarder->szTagName, sizeof(pGuarder->szTagName), pGuarderList[i].szUserName,
											strlen(pGuarderList[i].szUserName));
										strncpy_s(pGuarder->szPassword, sizeof(pGuarder->szPassword), pGuarderList[i].szPasswd,
											strlen(pGuarderList[i].szPasswd));
										strncpy_s(pGuarder->szOrg, sizeof(pGuarder->szOrg), pGuarderList[i].szOrgId,
											strlen(pGuarderList[i].szOrgId));
										pGuarder->szBindDevice[0] = '\0';
										pGuarder->szCurrentSession[0] = '\0';
										pGuarder->szLink[0] = '\0';
										pGuarder->szTaskId[0] = '\0';
										pGuarder->szTaskStartTime[0] = '\0';
										pGuarder->uiState = STATE_GUARDER_FREE;
										zhash_update(g_guarderList, pGuarder->szId, pGuarder);
										zhash_freefn(g_guarderList, pGuarder->szId, free);
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen
											- nOffset, pGuarder, nGuarderSize);
										nOffset += nGuarderSize;
									}
								}
								pthread_mutex_unlock(&g_mutex4GuarderList);

								bufferUpdateInfo.pUpdateData[bufferUpdateInfo.uiUpdateDataLen] = '\0';
								bufferUpdateInfo.ulUpdateTime = (unsigned long)time(NULL);
								size_t nBufferUpdateSize = sizeof(BufferUpdate);
								unsigned short usUpdatePipeSequence = getNextPipeSequence();
								MessagePayload msgPayload;
								size_t nPayloadSize = sizeof(MessagePayload);
								MAKE_PAYLOAD_MARK(msgPayload.szMsgMark);
								MAKE_PAYLOAD_VERSION(msgPayload.szMsgVersion);
								msgPayload.usMsgType = MSG_BUFFER_MODIFY;
								msgPayload.usMsgSequence = usUpdatePipeSequence;
								msgPayload.uiMsgCount = 1;
								msgPayload.uiMsgDataLength = nBufferUpdateSize + bufferUpdateInfo.uiUpdateDataLen;
								msgPayload.pMsgData = (unsigned char *)zmalloc(msgPayload.uiMsgDataLength + 1);
								memcpy_s(msgPayload.pMsgData, msgPayload.uiMsgDataLength, &bufferUpdateInfo,
									nBufferUpdateSize);
								memcpy_s(msgPayload.pMsgData + nBufferUpdateSize, bufferUpdateInfo.uiUpdateDataLen + 1,
									bufferUpdateInfo.pUpdateData, bufferUpdateInfo.uiUpdateDataLen);
								msgPayload.pMsgData[msgPayload.uiMsgDataLength] = '\0';
								size_t nFrameDataLen = nPayloadSize + msgPayload.uiMsgDataLength;
								unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
								memcpy_s(pFrameData, nFrameDataLen, &msgPayload, nPayloadSize);
								memcpy_s(pFrameData + nPayloadSize, msgPayload.uiMsgDataLength + 1,
									msgPayload.pMsgData, msgPayload.uiMsgDataLength);
								pFrameData[nFrameDataLen] = '\0';
								zmsg_t * msg_update = zmsg_new();
								zframe_t * frame_update = zframe_new(pFrameData, nFrameDataLen);
								zmsg_append(msg_update, &frame_update);
								zmsg_send(&msg_update, m_pipeline);

								free(pGuarderList);
								pGuarderList = NULL;
								if (bufferUpdateInfo.pUpdateData && bufferUpdateInfo.uiUpdateDataLen) {
									free(bufferUpdateInfo.pUpdateData);
									bufferUpdateInfo.pUpdateData = NULL;
									bufferUpdateInfo.uiUpdateDataLen = 0;
								}
								if (msgPayload.pMsgData && msgPayload.uiMsgDataLength) {
									free(msgPayload.pMsgData);
									msgPayload.pMsgData = NULL;
									msgPayload.uiMsgDataLength = 0;
								}
								free(pFrameData);
								pFrameData = NULL;

								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update GuarderList sql at %s\r\n",
									__FUNCTION__, __LINE__, szLastUpdateTime);
								writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
							}
							mysql_free_result(res_ptr);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update GuarderList sql at %s failed,"
							" error=%d:%s\r\n", __FUNCTION__, __LINE__, szLastUpdateTime, mysql_errno(m_updateConn),
							mysql_error(m_updateConn));
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
						break;
					}
					nErr = mysql_real_query(m_updateConn, szDeviceSql, ulDeviceSqlLen);
					if (nErr == 0) {
						res_ptr = mysql_store_result(m_updateConn);
						if (res_ptr) {
							my_ulonglong nRowCount = mysql_num_rows(res_ptr);
							if (nRowCount > 0) {
								MYSQL_ROW row;
								size_t nCount = (size_t)nRowCount;
								size_t nSqlDeviceSize = sizeof(escort_db::SqlDevice);
								escort_db::SqlDevice * pDeviceList = (escort_db::SqlDevice *)zmalloc(
									nCount * nSqlDeviceSize);
								size_t i = 0;
								while (row = mysql_fetch_row(res_ptr)) {
									strncpy_s(pDeviceList[i].szDeviceId, sizeof(pDeviceList[i].szDeviceId),
										row[0], strlen(row[0]));
									strncpy_s(pDeviceList[i].szFactoryId, sizeof(pDeviceList[i].szFactoryId),
										row[1], strlen(row[1]));
									strncpy_s(pDeviceList[i].szOrgId, sizeof(pDeviceList[i].szOrgId),
										row[2], strlen(row[2]));
									if (row[3] && strlen(row[3])) {
										strncpy_s(pDeviceList[i].szLastCommuncation,
											sizeof(pDeviceList[i].szLastCommuncation), row[3], strlen(row[3]));
									}
									if (row[4] && strlen(row[4])) {
										strncpy_s(pDeviceList[i].szLastLocation, sizeof(pDeviceList[i].szLastLocation),
											row[4], strlen(row[4]));
									}
									if (row[5]) {
										pDeviceList[i].dLat = atof(row[5]);
									}
									if (row[6]) {
										pDeviceList[i].dLng = atof(row[6]);
									}
									if (row[7]) {
										pDeviceList[i].nLocationType = atoi(row[7]);
									}
									if (row[8]) {
										pDeviceList[i].usIsUse = (unsigned short)atoi(row[8]);
									}
									if (row[9]) {
										pDeviceList[i].usBattery = (unsigned short)atoi(row[9]);
									}
									if (row[10]) {
										pDeviceList[i].usOnline = (unsigned short)atoi(row[10]);
									}
									if (row[11]) {
										pDeviceList[i].usIsRemove = (unsigned short)atoi(row[11]);
									}
									i++;
								}

								BufferUpdate bufferUpdateInfo;
								bufferUpdateInfo.usUpdateObject = BUFFER_DEVICE;
								bufferUpdateInfo.usUpdateDataCount = (unsigned short)nCount;
								size_t nDeviceSize = sizeof(WristletDevice);
								bufferUpdateInfo.uiUpdateDataLen = nCount * nDeviceSize;
								bufferUpdateInfo.pUpdateData = (unsigned char *)zmalloc(
									bufferUpdateInfo.uiUpdateDataLen + 1);
								size_t nOffset = 0;
								pthread_mutex_lock(&g_mutex4DevList);
								for (size_t i = 0; i < nCount; i++) {
									WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList,
										pDeviceList[i].szDeviceId);
									if (pDevice) { //check option: factoryId, OrgId, battery
										if (strcmp(pDevice->deviceBasic.szFactoryId, pDeviceList[i].szFactoryId) != 0) {
											strncpy_s(pDevice->deviceBasic.szFactoryId, sizeof(pDevice->deviceBasic.szFactoryId),
												pDeviceList[i].szFactoryId, strlen(pDeviceList[i].szFactoryId));
										}
										if (strcmp(pDevice->szOrganization, pDeviceList[i].szOrgId) != 0) {
											strncpy_s(pDevice->szOrganization, sizeof(pDevice->szOrganization),
												pDeviceList[i].szOrgId, strlen(pDeviceList[i].szFactoryId));
										}
										if (pDevice->deviceBasic.nBattery != pDeviceList[i].usBattery) {
											pDevice->deviceBasic.nBattery = pDeviceList[i].usBattery;
										}
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen - nOffset,
											pDevice, nDeviceSize);
										nOffset += nDeviceSize;
									}
									else {
										pDevice = (WristletDevice *)zmalloc(nDeviceSize);
										memset(pDevice, 0, nDeviceSize);
										strncpy_s(pDevice->deviceBasic.szDeviceId, sizeof(pDevice->deviceBasic.szDeviceId),
											pDeviceList[i].szDeviceId, strlen(pDeviceList[i].szDeviceId));
										strncpy_s(pDevice->deviceBasic.szFactoryId, sizeof(pDevice->deviceBasic.szFactoryId),
											pDeviceList[i].szFactoryId, strlen(pDeviceList[i].szFactoryId));
										strncpy_s(pDevice->szOrganization, sizeof(pDevice->szOrganization),
											pDeviceList[i].szOrgId, strlen(pDeviceList[i].szOrgId));
										pDevice->deviceBasic.ulLastActiveTime = 0;
										pDevice->deviceBasic.nBattery = pDeviceList[i].usBattery;
										if (pDevice->deviceBasic.nBattery == 0) {
											pDevice->deviceBasic.nStatus = DEV_OFFLINE;
										}
										else {
											if (pDevice->deviceBasic.nBattery < 20) {
												pDevice->deviceBasic.nStatus = DEV_ONLINE + DEV_LOWPOWER;
											}
											else {
												pDevice->deviceBasic.nStatus = DEV_ONLINE;
											}
										}
										pDevice->deviceBasic.ulLastActiveTime = 0;
										pDevice->szBindGuard[0] = '\0';
										pDevice->szLinkId[0] = '\0';
										pDevice->nLastLocateType = pDeviceList[0].nLocationType;
										pDevice->ulBindTime = 0;
										pDevice->ulLastFleeAlertTime = 0;
										pDevice->ulLastLocateTime = 0;
										pDevice->ulLastLooseAlertTime = 0;
										pDevice->ulLastLowPowerAlertTime = 0;
										if (pDevice->nLastLocateType == escort_db::E_LOCATE_APP) {
											pDevice->guardPosition.dLatitude = pDeviceList[i].dLat;
											pDevice->guardPosition.dLngitude = pDeviceList[i].dLng;
											pDevice->guardPosition.usLatType = pDevice->guardPosition.usLngType = 1;
											pDevice->devicePosition.dLatitude = pDevice->devicePosition.dLngitude = 0.000000;
											pDevice->devicePosition.usLatType = pDevice->devicePosition.usLngType = 1;
											pDevice->devicePosition.nPrecision = pDevice->guardPosition.nPrecision = 0;
										}
										else {
											pDevice->devicePosition.dLatitude = pDeviceList[i].dLat;
											pDevice->devicePosition.dLngitude = pDeviceList[i].dLng;
											pDevice->devicePosition.usLatType = pDevice->devicePosition.usLngType = 1;
											pDevice->guardPosition.dLatitude = pDevice->guardPosition.dLngitude = 0.000000;
											pDevice->guardPosition.usLatType = pDevice->guardPosition.usLngType = 1;
											pDevice->devicePosition.nPrecision = pDevice->guardPosition.nPrecision = 0;
										}
										zhash_update(g_deviceList, pDevice->deviceBasic.szDeviceId, pDevice);
										zhash_freefn(g_deviceList, pDevice->deviceBasic.szDeviceId, free);
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen - nOffset,
											pDevice, nDeviceSize);
										nOffset += nDeviceSize;
									}
								}
								pthread_mutex_unlock(&g_mutex4DevList);
								bufferUpdateInfo.ulUpdateTime = (unsigned long)time(NULL);
								bufferUpdateInfo.pUpdateData[bufferUpdateInfo.uiUpdateDataLen] = '\0';
								size_t nBufferUpdateSize = sizeof(BufferUpdate);
								MessagePayload msgPayload;
								size_t nPayloadSize = sizeof(MessagePayload);
								MAKE_PAYLOAD_MARK(msgPayload.szMsgMark);
								MAKE_PAYLOAD_VERSION(msgPayload.szMsgVersion);
								msgPayload.uiMsgCount = 1;
								msgPayload.usMsgType = MSG_BUFFER_MODIFY;
								msgPayload.usMsgSequence = getNextPipeSequence();
								msgPayload.uiMsgDataLength = nBufferUpdateSize + bufferUpdateInfo.uiUpdateDataLen;
								msgPayload.pMsgData = (unsigned char *)zmalloc(msgPayload.uiMsgDataLength + 1);
								memcpy_s(msgPayload.pMsgData, msgPayload.uiMsgDataLength, &bufferUpdateInfo, nBufferUpdateSize);
								memcpy_s(msgPayload.pMsgData + nBufferUpdateSize, bufferUpdateInfo.uiUpdateDataLen + 1,
									bufferUpdateInfo.pUpdateData, bufferUpdateInfo.uiUpdateDataLen);
								msgPayload.pMsgData[msgPayload.uiMsgDataLength] = '\0';
								size_t nFrameDataLen = nPayloadSize + msgPayload.uiMsgDataLength;
								unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
								memcpy_s(pFrameData, nFrameDataLen, &msgPayload, nPayloadSize);
								memcpy_s(pFrameData + nPayloadSize, msgPayload.uiMsgDataLength + 1, msgPayload.pMsgData,
									msgPayload.uiMsgDataLength);
								pFrameData[nFrameDataLen] = '\0';
								zmsg_t * msg_update = zmsg_new();
								zframe_t * frame_update = zframe_new(pFrameData, nFrameDataLen);
								zmsg_append(msg_update, &frame_update);
								zmsg_send(&msg_update, m_pipeline);

								free(pDeviceList);
								pDeviceList = NULL;
								if (bufferUpdateInfo.pUpdateData && bufferUpdateInfo.uiUpdateDataLen) {
									free(bufferUpdateInfo.pUpdateData);
									bufferUpdateInfo.pUpdateData = NULL;
									bufferUpdateInfo.uiUpdateDataLen = 0;
								}
								if (msgPayload.pMsgData && msgPayload.uiMsgDataLength) {
									free(msgPayload.pMsgData);
									msgPayload.pMsgData = NULL;
									msgPayload.uiMsgDataLength = 0;
								}
								free(pFrameData);
								pFrameData = NULL;

								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update DeviceList sql at %s\r\n",
									__FUNCTION__, __LINE__, szLastUpdateTime);
								writeLog(szLog, LOGCATEGORY_INFORMATION, m_nLogType);
							}
							mysql_free_result(res_ptr);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update DeviceList Sql at %s failed,"
							" error=%d,%s\r\n", __FUNCTION__, __LINE__, szLastUpdateTime, mysql_errno(m_updateConn),
							mysql_error(m_updateConn));
						writeLog(szLog, LOGCATEGORY_EXCEPTION, m_nLogType);
					}
				}
			} while (0);
			g_ulLastUpdateTime = pTask->ulUpdateTaskTime;
			g_nUpdatePipeState = dbproxy::E_PIPE_CLOSE;
			free(pTask);
			pTask = NULL;
		}	
	} while (1);
}

bool DbProxy::addUpdateTask(dbproxy::UpdatePipeTask * pTask_)
{
	bool result = false;
	if (pTask_) {
		pthread_mutex_lock(&m_mutex4UpdatePipe);
		m_updateTaskQue.push(pTask_);
		if (m_updateTaskQue.size() == 1) {
			pthread_cond_signal(&m_cond4UpdatePipe);
		}
		result = true;
		pthread_mutex_unlock(&m_mutex4UpdatePipe);
	}
	return result;
}

void * dealLogThread(void * param_)
{
	DbProxy * pProxy = (DbProxy *)param_;
	if (pProxy) {
		pProxy->dealLog();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealSqlQueryThread(void * param_)
{
	DbProxy * pProxy = (DbProxy *)param_;
	if (pProxy) {
		pProxy->dealSqlQuery();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealSqlExecThread(void * param_)
{
	DbProxy * pProxy = (DbProxy *)param_;
	if (pProxy) {
		pProxy->dealSqlExec();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealSqlLocateThread(void * param_)
{
	DbProxy * pProxy = (DbProxy *)param_;
	if (pProxy) {
		pProxy->dealSqlLocate();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealNetworkThread(void * param_)
{
	DbProxy * pProxy = (DbProxy *)param_;
	if (pProxy) {
		pProxy->dealNetwork();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealTopicMsgThread(void * param_)
{
	DbProxy * pProxy = (DbProxy *)param_;
	if (pProxy) {
		pProxy->dealTopicMsg();
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealInteractMsgThread(void * param_)
{
	DbProxy * pProxy = (DbProxy *)param_;
	if (pProxy) {
		pProxy->dealInteractMsg();
	}
	pthread_exit(NULL);
	return NULL;
}

void * superviseThread(void * param_)
{
	DbProxy * pProxy = (DbProxy *)param_;
	if (pProxy) {
		zloop_start(pProxy->m_loop);
	}
	pthread_exit(NULL);
	return NULL;
}

void * dealUpdatePipeThread(void * param_)
{
	DbProxy * pProxy = (DbProxy *)param_;
	if (pProxy) {
		pProxy->updatePipeLoop();
	}
	pthread_exit(NULL);
	return NULL;
}

void zk_server_watcher(zhandle_t * zh_, int type_, int state_, const char * path_,
	void * watcherCtx_)
{
	DbProxy * pInst = (DbProxy *)watcherCtx_;
	if (type_ == ZOO_SESSION_EVENT) {
		if (state_ == ZOO_CONNECTED_STATE) {
			if (pInst) {
				pInst->m_bZKConnected = true;
			}
		}
		else if (state_ == ZOO_EXPIRED_SESSION_STATE) {
			if (pInst) {
				pInst->m_bZKConnected = false;
				zookeeper_close(pInst->m_zkHandle);
				pInst->m_zkHandle = NULL;
			}
		}
	}
}

void zk_escort_create_completion(int rc_, const char * name_, const void * data_)
{
	DbProxy * pInst = (DbProxy *)data_;
	switch (rc_) {
		case ZCONNECTIONLOSS:
		case ZOPERATIONTIMEOUT: {
			if (pInst && pInst->m_nRun) {
				zoo_acreate(pInst->m_zkHandle, "/escort", "", 0, &ZOO_OPEN_ACL_UNSAFE, 0,
					zk_escort_create_completion, data_);
			}
			break;
		}
		case ZOK: {
			fprintf(stdout, "zookeeper create /escort ok\n");
			break;
		}
		case ZNODEEXISTS: {
			fprintf(stdout, "zookeeper /escort exists\n");
			break;
		}
	}
}

void zk_dbproxy_create_completion(int rc_, const char * name_, const void * data_)
{
	DbProxy * pInst = (DbProxy *)data_;
	switch (rc_) {
		case ZCONNECTIONLOSS:
		case ZOPERATIONTIMEOUT: {
			if (pInst && pInst->m_nRun) {
				zoo_acreate(pInst->m_zkHandle, "/escort/dbproxy", "", 1024, &ZOO_OPEN_ACL_UNSAFE, 0,
					zk_dbproxy_create_completion, data_);
			}
			break;
		}
		case ZOK: {
			fprintf(stdout, "zookeeper create /escort/dbproxy ok\n");
			break;
		}
		case ZNODEEXISTS: {
			fprintf(stdout, "zookeeper /escort/dbproxy exists\n");
			break;
		}
	}
}

void zk_dbproxy_master_create_completion(int rc_, const char * name_, const void * data_)
{
	DbProxy * pInst = (DbProxy *)data_;
	switch (rc_) {
		case ZCONNECTIONLOSS:
		case ZOPERATIONTIMEOUT: {
			if (pInst && pInst->m_nRun) {
				pInst->competeForMaster();
			}
			break;
		}
		case ZOK: {
			if (pInst) {
				if (pInst->m_zkDbProxy.usRank == 0 && strlen(pInst->m_zkNodePath)) {
					pInst->removeSlaver();
				}
				size_t nSize = strlen(name_);
				memcpy_s(pInst->m_zkNodePath, sizeof(pInst->m_zkNodePath), name_, nSize);
				pInst->m_zkNodePath[nSize] = '\0';
				pInst->m_zkDbProxy.usRank = 1;
				pInst->setZkDbProxyData(name_, &pInst->m_zkDbProxy, sizeof(ZkDatabaseProxy));
			}
			break;
		}
		case ZNODEEXISTS: {
			if (pInst) {
				pInst->masterExist();
				pInst->runAsSlaver();
			}
			break;
		}
	}
}

void zk_dbproxy_master_exists_watcher(zhandle_t * zh_, int type_, int state_,
	const char * path_, void * watcherCtx_)
{
	DbProxy * pInst = (DbProxy *)watcherCtx_;
	if (type_ == ZOO_DELETED_EVENT) {
		if (pInst) {
			if (pInst->m_nRun) {
				pInst->competeForMaster();
			}
		}
	}
}

void zk_dbproxy_master_exists_completion(int rc_, const Stat * stat_, const void * data_)
{
	DbProxy * pInst = (DbProxy *)data_;
	switch (rc_) {
		case ZCONNECTIONLOSS: 
		case ZOPERATIONTIMEOUT: {
			if (pInst && pInst->m_nRun) {
				pInst->masterExist();
			}
			break;
		}
		case ZOK: {
			break;
		}
		case ZNONODE: {
			if (pInst && pInst->m_nRun) {
				pInst->competeForMaster();
			}
		}
	}
}

void zk_dbproxy_slaver_create_completion(int rc_, const char * name_, const void * data_)
{
	DbProxy * pInst = (DbProxy *)data_;
	switch (rc_) {
		case ZCONNECTIONLOSS:
		case ZOPERATIONTIMEOUT: {
			if (pInst && pInst->m_nRun) {
				pInst->runAsSlaver();
			}
			break;
		}
		case ZOK: {
			if (pInst && pInst->m_nRun) {
				size_t nSize = strlen(name_);
				memcpy_s(pInst->m_zkNodePath, sizeof(pInst->m_zkNodePath), name_, nSize);
				pInst->m_zkNodePath[nSize] = '\0';
				pInst->m_zkDbProxy.usRank = 0;
				pInst->setZkDbProxyData(name_, &pInst->m_zkDbProxy, sizeof(ZkDatabaseProxy));
			}
			break;
		}
	}
}

void zk_dbproxy_set_completion(int rc_, const Stat * stat_ , const void * data_)
{
	DbProxy * pInst = (DbProxy *)data_;
	switch (rc_) {
		case ZCONNECTIONLOSS:
		case ZOPERATIONTIMEOUT: {
			if (pInst && pInst->m_nRun) {
				pInst->setZkDbProxyData(pInst->m_zkNodePath, &pInst->m_zkDbProxy, sizeof(ZkDatabaseProxy));
			}
			break;
		}
		case ZOK: {
			break;
		}
		case ZNONODE: {
			break;
		}
	}
}

