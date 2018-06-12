#include "DbProxyConcrete.h"

zhash_t * DbProxy::g_deviceList = NULL;
zhash_t * DbProxy::g_guarderList = NULL;
zhash_t * DbProxy::g_taskList = NULL;
zhash_t * DbProxy::g_personList = NULL;
zhash_t * DbProxy::g_orgList = NULL;
zhash_t * DbProxy::g_fenceList = NULL;
zhash_t * DbProxy::g_fenceTaskList = NULL;
pthread_mutex_t DbProxy::g_mutex4DevList;
pthread_mutex_t DbProxy::g_mutex4GuarderList;
pthread_mutex_t DbProxy::g_mutex4TaskList;
pthread_mutex_t DbProxy::g_mutex4PersonList;
pthread_mutex_t DbProxy::g_mutex4OrgList;
pthread_mutex_t DbProxy::g_mutex4InteractSequence;
pthread_mutex_t DbProxy::g_mutex4PipeSequence;
pthread_mutex_t DbProxy::g_mutex4UpdateTime;
pthread_mutex_t DbProxy::g_mutex4PipeState;
pthread_mutex_t DbProxy::g_mutex4FenceList;
pthread_mutex_t DbProxy::g_mutex4FenceTaskList;
int DbProxy::g_nRefCount = 0;
unsigned int DbProxy::g_uiInteractSequence = 0;
unsigned int DbProxy::g_uiPipeSequence = 0;
BOOL DbProxy::g_bLoadSql = FALSE;
//char DbProxy::g_szLastUpdateTime[20] = { 0 };
int DbProxy::g_nUpdatePipeState = 0;
unsigned long long DbProxy::g_ulLastUpdateTime = 0;

static unsigned long long strdatetime2time(const char * strDatetime)
{
	if (strDatetime) {
		struct tm tm_curr;
		sscanf_s(strDatetime, "%04d%02d%02d%02d%02d%02d", &tm_curr.tm_year, &tm_curr.tm_mon, 
			&tm_curr.tm_mday, &tm_curr.tm_hour, &tm_curr.tm_min, &tm_curr.tm_sec);
		tm_curr.tm_year -= 1900;
		tm_curr.tm_mon -= 1;
		return (unsigned long long)mktime(&tm_curr);
	}
	return 0;
}

static unsigned long long sqldatetime2time(const char * sqlDatetime)
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

static void format_datetime(unsigned long long ulSrcTime, char * pStrDatetime, size_t nStrDatetimeLen)
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

static void format_sqldatetime(unsigned long long ulSrcTime, char * pSqlDatetime, size_t nDatetimeLen)
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
	size_t nTotalLen = 0;
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
	m_locateConn = NULL;
	m_updateConn = NULL;
	pthread_mutex_init(&m_mutex4ReadConn, NULL);
	pthread_mutex_init(&m_mutex4WriteConn, NULL);
	pthread_mutex_init(&m_mutex4LocateConn, NULL);
	pthread_mutex_init(&m_mutex4UpdateConn, NULL);

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
		g_fenceList = zhash_new();
		g_fenceTaskList = zhash_new();
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
		pthread_mutex_init(&g_mutex4PipeState, NULL);
		pthread_mutex_init(&g_mutex4FenceList, NULL);
		pthread_mutex_init(&g_mutex4FenceTaskList, NULL);
	}
	g_nRefCount++;

	m_ullLogInst = 0;
	m_usLogType = pf_logger::eLOGTYPE_FILE;
	m_szLogRoot[0] = '\0';
	 
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
		if (g_fenceList) {
			zhash_destroy(&g_fenceList);
		}
		if (g_fenceTaskList) {
			zhash_destroy(&g_fenceTaskList);
		}
		pthread_mutex_destroy(&g_mutex4DevList);
		pthread_mutex_destroy(&g_mutex4GuarderList);
		pthread_mutex_destroy(&g_mutex4TaskList);
		pthread_mutex_destroy(&g_mutex4PersonList);
		pthread_mutex_destroy(&g_mutex4OrgList);
		pthread_mutex_destroy(&g_mutex4InteractSequence);
		pthread_mutex_destroy(&g_mutex4PipeSequence);
		pthread_mutex_destroy(&g_mutex4UpdateTime);
		pthread_mutex_destroy(&g_mutex4PipeState);
		pthread_mutex_destroy(&g_mutex4FenceList);
		pthread_mutex_destroy(&g_mutex4FenceTaskList);
		g_nRefCount = 0;
	}

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
	pthread_mutex_destroy(&m_mutex4WriteConn);
	pthread_mutex_destroy(&m_mutex4LocateConn);
	pthread_mutex_destroy(&m_mutex4UpdateConn);
	pthread_mutex_destroy(&m_mutex4UpdatePipe);
	pthread_cond_destroy(&m_cond4UpdatePipe);
	


	if (m_ullLogInst) {
		LOG_Release(m_ullLogInst);
		m_ullLogInst = 0;
	}
	if (m_zkHandle) {
		zookeeper_close(m_zkHandle);
		m_zkHandle = NULL;
	}
	mysql_library_end();
	zsys_shutdown();
}

int DbProxy::Start(const char * pHost_, unsigned short usReceptPort_, const char * pMidwareHost_, 
	unsigned short usPublishPort_, unsigned short usContactPort_, unsigned short usCollectPort_,
	const char * pMasterDbHost_, const char * pMasterDbUser_, const char * pMasterDbPasswd_, 
	unsigned short usMasterDbPort_, const char * pSlaveDbHost_, const char * pSlaveDbUser_, 
	const char * pSlaveDbPasswd_, unsigned short usSlaveDbPort_, const char * pDbName_, const char * pDbAuxName_)
{
	if (m_nRun) {
		return 0;
	}
	memset(&m_remoteLink, 0, sizeof(m_remoteLink));

	int err = 0;
	m_interactor = zsock_new(ZMQ_DEALER);
	char szUuid[64] = { 0 };
	sprintf_s(szUuid, sizeof(szUuid), "dbit-%x-%04x-%04x", (unsigned int)time(NULL), (rand() % 10000), 
		(rand() % 10000));
	zsock_set_identity(m_interactor, szUuid);
	err = zsock_connect(m_interactor, "tcp://%s:%u", strlen(pMidwareHost_) ? pMidwareHost_ : "127.0.0.1",
		usContactPort_ > 0 ? usContactPort_ : 25001);

	m_subscriber = zsock_new(ZMQ_SUB);
	zsock_set_subscribe(m_subscriber, "");
	err = zsock_connect(m_subscriber, "tcp://%s:%u", strlen(pMidwareHost_) ? pMidwareHost_ : "127.0.0.1",
		usPublishPort_ > 0 ? usPublishPort_ : 25000);

	m_pipeline = zsock_new(ZMQ_DEALER);
	sprintf_s(m_szPipelineIdentity, sizeof(m_szPipelineIdentity), "dbpipe-%x-%04x-%04x", (unsigned int)time(NULL),
		(rand() % 10000), (rand() % 10000));
	zsock_set_identity(m_pipeline, m_szPipelineIdentity);
	err = zsock_connect(m_pipeline, "tcp://%s:%u", strlen(pMidwareHost_) ? pMidwareHost_ : "127.0.0.1",
		usCollectPort_ > 0 ? usCollectPort_ : 25002);

	m_reception = zsock_new(ZMQ_ROUTER);
	zsock_set_router_handover(m_reception, 1);
	err = zsock_bind(m_reception, "tcp://*:%u", usReceptPort_);

	m_readConn = mysql_init(NULL);
	m_writeConn = mysql_init(NULL);
	m_locateConn = mysql_init(NULL);
	m_updateConn = mysql_init(NULL);
	if (m_readConn && m_writeConn && m_locateConn && m_updateConn) {
		if (mysql_real_connect(m_readConn, (pSlaveDbHost_ && strlen(pSlaveDbHost_)) ? pSlaveDbHost_ : "127.0.0.1", 
			pSlaveDbUser_, pSlaveDbPasswd_, (pDbName_ && strlen(pDbName_)) ? pDbName_ : DBNAME_MAJOR, 
			(usSlaveDbPort_ == 0) ? 3306 : usSlaveDbPort_, NULL, 0) 
			&& mysql_real_connect(m_writeConn, (pMasterDbHost_ && strlen(pMasterDbHost_)) ? pMasterDbHost_ : "127.0.0.1",
			pMasterDbUser_, pMasterDbPasswd_, (pDbName_ && strlen(pDbName_)) ? pDbName_ : DBNAME_MAJOR, 
				(usMasterDbPort_ == 0) ? 3306 : usMasterDbPort_, NULL, 0)
			&& mysql_real_connect(m_locateConn, (pMasterDbHost_ && strlen(pMasterDbHost_)) ? pMasterDbHost_ : "127.0.0.1", 
			pMasterDbUser_, pMasterDbPasswd_, (pDbAuxName_ && strlen(pDbAuxName_)) ? pDbAuxName_ : DBNAME_LOCATE, 
			(usMasterDbPort_ == 0) ? 3306 : usMasterDbPort_, NULL, 0) 
			&& mysql_real_connect(m_updateConn, (pMasterDbHost_ && strlen(pMasterDbHost_)) ? pMasterDbHost_ : "127.0.0.1",
			pMasterDbUser_, pMasterDbPasswd_, (pDbName_ && strlen(pDbName_)) ? pDbName_ : DBNAME_MAJOR,
			(usMasterDbPort_ == 0) ? 3306 : usMasterDbPort_, NULL, 0)) {
			mysql_set_character_set(m_readConn, "gb2312");
			mysql_set_character_set(m_writeConn, "gb2312");
			mysql_set_character_set(m_locateConn, "gb2312");
			mysql_set_character_set(m_updateConn, "gb2312");

			m_nRun = 1;
			m_nTimerTickCount = 0;
			m_loop = zloop_new();
			m_nTimer4Supervise = zloop_timer(m_loop, 1000, 0, supervise, this);
			zloop_reader(m_loop, m_subscriber, readSubscriber, this);
			zloop_reader_set_tolerant(m_loop, m_subscriber);
			zloop_reader(m_loop, m_interactor, readInteractor, this);
			zloop_reader_set_tolerant(m_loop, m_interactor);
			zloop_reader(m_loop, m_pipeline, readPipeline, this);
			zloop_reader_set_tolerant(m_loop, m_pipeline);
			zloop_reader(m_loop, m_reception, readReception, this);
			zloop_reader_set_tolerant(m_loop, m_reception);

			//if (m_pthdNetwork.p == NULL) {
			//	pthread_create(&m_pthdNetwork, NULL, dealNetworkThread, this);
			//}
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
				strncpy_s(m_zkDbProxy.szProxyHostIp, sizeof(m_zkDbProxy.szProxyHostIp), pHost_, strlen(pHost_));
			}
			else {
				snprintf(m_zkDbProxy.szProxyHostIp, sizeof(m_zkDbProxy.szProxyHostIp), "127.0.0.1");
			}
			if (pMasterDbHost_ && strlen(pMasterDbHost_)) {
				strncpy_s(m_zkDbProxy.szDbHostIp, sizeof(m_zkDbProxy.szDbHostIp), pMasterDbHost_, strlen(pMasterDbHost_));
			}
			if (pMasterDbUser_ && strlen(pMasterDbUser_)) {
				strncpy_s(m_zkDbProxy.szDbUser, sizeof(m_zkDbProxy.szDbUser), pMasterDbUser_, strlen(pMasterDbUser_));
			}
			if (pMasterDbPasswd_ && strlen(pMasterDbPasswd_)) {
				strncpy_s(m_zkDbProxy.szDbPasswd, sizeof(m_zkDbProxy.szDbPasswd), pMasterDbPasswd_, strlen(pMasterDbPasswd_));
			}
			if (pSlaveDbHost_ && strlen(pSlaveDbHost_)) {
				strncpy_s(m_zkDbProxy.szSlaveDbHostIp, sizeof(m_zkDbProxy.szSlaveDbHostIp), pSlaveDbHost_, strlen(pSlaveDbHost_));
			}
			if (pSlaveDbUser_ && strlen(pSlaveDbUser_)) {
				strcpy_s(m_zkDbProxy.szSlaveDbUser, sizeof(m_zkDbProxy.szSlaveDbUser), pSlaveDbUser_);
			}
			if (pSlaveDbPasswd_ && strlen(pSlaveDbPasswd_)) {
				strcpy_s(m_zkDbProxy.szSlaveDbPasswd, sizeof(m_zkDbProxy.szSlaveDbPasswd), pSlaveDbPasswd_);
			}
			if (pDbName_ && strlen(pDbName_)) {
				strncpy_s(m_zkDbProxy.szMajorSample, sizeof(m_zkDbProxy.szMajorSample), pDbName_, strlen(pDbName_));
			}
			if (pDbAuxName_ && strlen(pDbAuxName_)) {
				strncpy_s(m_zkDbProxy.szLocateSample, sizeof(m_zkDbProxy.szLocateSample), pDbAuxName_, strlen(pDbAuxName_));
			}
			m_zkDbProxy.usMasterDbPort = usMasterDbPort_;
			m_zkDbProxy.usSlaveDbPort = usSlaveDbPort_;
			competeForMaster();
			if (g_bLoadSql == FALSE) {
				if (initSqlBuffer()) {
					unsigned long long ulTime = (unsigned long long)time(NULL);
					g_ulLastUpdateTime = ulTime;
					g_bLoadSql = TRUE;
				}
			}
			char szLog[256] = { 0 };
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]Database proxy start %u\r\n", __FUNCTION__, __LINE__, usReceptPort_);
			LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			return 0;
		}
	}
	if (m_subscriber) {
		zsock_destroy(&m_subscriber);
	}
	if (m_reception) {
		zsock_destroy(&m_reception);
	}
	if (m_interactor) {
		zsock_destroy(&m_interactor);
	}
	if (m_pipeline) {
		zsock_destroy(&m_pipeline);
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
	sprintf_s(szLog, sizeof(szLog), "[DbProxy]%s[%d]Database proxy stop\r\n", __FUNCTION__, __LINE__);
	LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
	if (m_pthdSupervise.p) {
		pthread_join(m_pthdSupervise, NULL);
		m_pthdSupervise.p = NULL;
	}
	if (m_pthdNetwork.p) {
		pthread_join(m_pthdNetwork, NULL);
		m_pthdNetwork.p = NULL;
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
	if (m_loop) {
		zloop_destroy(&m_loop);
	}
	if (m_subscriber) {
		zsock_destroy(&m_subscriber);
	}
	if (m_reception) {
		zsock_destroy(&m_reception);
	}
	if (m_interactor) {
		zsock_destroy(&m_interactor);
	}
	if (m_pipeline) {
		zsock_destroy(&m_pipeline);
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
		m_updateConn = NULL;
	}
	return 0;
}

int DbProxy::GetState()
{
	return m_nRun;
}

void DbProxy::SetLogType(unsigned short usLogType_)
{
	if (m_ullLogInst) {
		if (m_usLogType != usLogType_) {
			pf_logger::LogConfig logConf;
			LOG_GetConfig(m_ullLogInst, &logConf);
			if (logConf.usLogType != usLogType_) {
				logConf.usLogType = usLogType_;
				LOG_SetConfig(m_ullLogInst, logConf);
			}
			m_usLogType = logConf.usLogType;
		}
	}
}

void DbProxy::initLog()
{
	if (m_ullLogInst == 0) {
		m_ullLogInst = LOG_Init();
		if (m_ullLogInst) {
			pf_logger::LogConfig logConf;
			logConf.usLogType = m_usLogType;
			logConf.usLogPriority = pf_logger::eLOGPRIO_ALL;
			char szLogDir[256] = { 0 };
			snprintf(szLogDir, 256, "%slog\\", m_szLogRoot);
			CreateDirectoryExA(".\\", szLogDir, NULL);
			strcat_s(szLogDir, 256, "escort_dbproxy\\");
			CreateDirectoryExA(".\\", szLogDir, NULL);
			strncpy_s(logConf.szLogPath, sizeof(logConf.szLogPath), szLogDir, strlen(szLogDir));
			LOG_SetConfig(m_ullLogInst, logConf);
		}
	}
}

bool DbProxy::addSqlTransaction(dbproxy::SqlTransaction * pTransaction_, int nTransactionType_)
{
	bool result = false;
	if (pTransaction_ && pTransaction_->pSqlList && pTransaction_->uiSqlCount) {
		if (nTransactionType_ == SQLTYPE_EXECUTE) {
			pthread_mutex_lock(&m_mutex4ExecQue);
			m_sqlExecuteQue.push(pTransaction_);
			if (m_sqlExecuteQue.size() == 1) {
				pthread_cond_broadcast(&m_cond4ExecQue);
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
					if (pTransaction->pSqlList[i].pStatement && pTransaction->pSqlList[i].uiStatementLen) {
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
					if (pTransaction->pSqlList[i].pStatement && pTransaction->pSqlList[i].uiStatementLen) {
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
					if (pTransaction->pSqlList[i].pStatement && pTransaction->pSqlList[i].uiStatementLen) {
						handleSqlLocate(&pTransaction->pSqlList[i], pTransaction->uiTransactionSequence, 
							pTransaction->ulTransactionTime);
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
						pTransaction->pSqlList[i].uiStatementLen = 0;
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
	unsigned long long ulQryTime_, const char * pQryFrom_)
{
	char szLog[1024] = { 0 };
	pthread_mutex_lock(&m_mutex4ReadConn);
	if (m_readConn && pSqlStatement_ && pSqlStatement_->pStatement && pSqlStatement_->uiStatementLen) {
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]seq=%u, time=%llu, execute query sql: %s\r\n",
			__FUNCTION__, __LINE__, uiQrySeq_, ulQryTime_, pSqlStatement_->pStatement);
		//LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		printf(szLog);
		int nErr = mysql_real_query(m_readConn, pSqlStatement_->pStatement, pSqlStatement_->uiStatementLen);
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
							memset(&pSqlDevList[i], 0, sizeof(escort_db::SqlDevice));
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
							if (row[12] && strlen(row[12])) {
								strcpy_s(pSqlDevList[i].szImei, sizeof(pSqlDevList[i].szImei), row[12]);
							}
							if (row[13] && strlen(row[13])) {
								pSqlDevList[i].nMnc = atoi(row[13]);
							}
							if (row[14] && strlen(row[14])) {
								pSqlDevList[i].nCoordinate = atoi(row[14]);
							}
							if (row[15] && strlen(row[15])) {
								pSqlDevList[i].nCharge = atoi(row[15]);
							}
							WristletDevice * pDev = (WristletDevice *)zmalloc(nDeviceSize);
							memset(pDev, 0, nDeviceSize);
							pDev->deviceBasic.nStatus = DEV_ONLINE;
							pDev->deviceBasic.nBattery = pSqlDevList[i].usBattery;
							if (pSqlDevList[i].usIsRemove) { //loose
								pDev->deviceBasic.nStatus += DEV_LOOSE;
								pDev->deviceBasic.nLooseStatus = 1;
							}
							else {
								pDev->deviceBasic.nLooseStatus = 0;
							}
							if (pDev->deviceBasic.nBattery < BATTERY_THRESHOLD) {
								pDev->deviceBasic.nStatus += DEV_LOWPOWER;
							}
							strncpy_s(pDev->deviceBasic.szDeviceId, sizeof(pDev->deviceBasic.szDeviceId),
								pSqlDevList[i].szDeviceId, strlen(pSqlDevList[i].szDeviceId));
							strncpy_s(pDev->deviceBasic.szFactoryId, sizeof(pDev->deviceBasic.szFactoryId),
								pSqlDevList[i].szFactoryId, strlen(pSqlDevList[i].szFactoryId));
							strncpy_s(pDev->deviceBasic.szOrgId, sizeof(pDev->deviceBasic.szOrgId), 
								pSqlDevList[i].szOrgId, strlen(pSqlDevList[i].szOrgId));
							if (pSqlDevList[i].nLocationType == escort_db::E_LOCATE_APP) {
								pDev->guardPosition.dLatitude = pSqlDevList[i].dLat;
								pDev->guardPosition.dLngitude = pSqlDevList[i].dLng;
								pDev->guardPosition.nCoordinate = (int8_t)pSqlDevList[i].nCoordinate;
								pDev->nLastLocateType = LOCATE_APP;
							}
							else {
								pDev->devicePosition.dLatitude = pSqlDevList[i].dLat;
								pDev->devicePosition.dLngitude = pSqlDevList[i].dLng;
								pDev->nLastLocateType = (pSqlDevList[i].nLocationType == escort_db::E_LOCATE_GPS) ?
									LOCATE_GPS : LOCATE_LBS;
								pDev->devicePosition.nCoordinate = (int8_t)pSqlDevList[i].nCoordinate;
							}
							if (strlen(pSqlDevList[i].szLastLocation)) {
								pDev->ulLastDeviceLocateTime = sqldatetime2time(pSqlDevList[i].szLastLocation);
							}
							if (strlen(pSqlDevList[i].szLastCommuncation)) {
								pDev->deviceBasic.ulLastActiveTime = sqldatetime2time(pSqlDevList[i].szLastCommuncation);
							}
							if (pSqlDevList[i].usOnline) {
								pDev->deviceBasic.nOnline = 1;
							} 
							else {
								pDev->deviceBasic.nOnline = 0;
							}
							strncpy_s(pDev->deviceBasic.szDeviceImei, sizeof(pDev->deviceBasic.szDeviceImei), pSqlDevList[i].szImei,
								strlen(pSqlDevList[i].szImei));
							pDev->deviceBasic.nDeviceMnc = pSqlDevList[i].nMnc;
							pDev->nDeviceInCharge = pSqlDevList[i].nCharge;
							memcpy_s(&pDevList[i], nDeviceSize, pDev, nDeviceSize);
							pthread_mutex_lock(&g_mutex4DevList);
							zhash_update(g_deviceList, pSqlDevList[i].szDeviceId, pDev);
							zhash_freefn(g_deviceList, pSqlDevList[i].szDeviceId, free);
							pthread_mutex_unlock(&g_mutex4DevList);
							sprintf_s(szLog, sizeof(szLog), "[DbProxy]%s[%d]load DeviceId=%s, factoryId=%s, orgId=%s, status=%hu,"
								" battery=%hu, looseState=%hu, online=%hu, imei=%s, mnc=%d, charge=%d\n", __FUNCTION__, __LINE__, 
								pDev->deviceBasic.szDeviceId, pDev->deviceBasic.szFactoryId, pDev->deviceBasic.szOrgId, 
								pDev->deviceBasic.nStatus, pDev->deviceBasic.nBattery, pDev->deviceBasic.nLooseStatus,
								pDev->deviceBasic.nOnline, pDev->deviceBasic.szDeviceImei, pDev->deviceBasic.nDeviceMnc, 
								pDev->nDeviceInCharge);
							LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							i++;
						}
						if (pQryFrom_ && strlen(pQryFrom_)) {
							replyQuery(pDevList, (unsigned int)nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, 
								ulQryTime_, pQryFrom_);
						}
						sprintf_s(szLog, sizeof(szLog), "[DbProxy]%s[%d]query row=%u, seq=%u, time=%llu, from=%s, sql=%s\r\n", 
							__FUNCTION__, __LINE__, (unsigned int)nCount, uiQrySeq_, ulQryTime_, pQryFrom_ ? pQryFrom_ : " ", 
							pSqlStatement_->pStatement);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
							if (row[4] && strlen(row[4])) {
								pSqlGuarderList[i].nUserRoleType = atoi(row[4]);
							}
							if (row[5] && strlen(row[5])) {
								strncpy_s(pSqlGuarderList[i].szPhoneCode, sizeof(pSqlGuarderList[i].szPhoneCode),
									row[5], strlen(row[5]));
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
							pGuarder->usState = STATE_GUARDER_FREE;
							pGuarder->usRoleType = (unsigned short)pSqlGuarderList[i].nUserRoleType;
							if (strlen(pSqlGuarderList[i].szPhoneCode)) {
								strncpy_s(pGuarder->szPhoneCode, sizeof(pGuarder->szPhoneCode), pSqlGuarderList[i].szPhoneCode,
									strlen(pSqlGuarderList[i].szPhoneCode));
							}
							memcpy_s(&pGuarderList[i], nGuarderSize, pGuarder, nGuarderSize);

							pthread_mutex_lock(&g_mutex4GuarderList);
							zhash_update(g_guarderList, pSqlGuarderList[i].szUserId, pGuarder);
							zhash_freefn(g_guarderList, pSqlGuarderList[i].szUserId, free);
							pthread_mutex_unlock(&g_mutex4GuarderList);

							sprintf_s(szLog, sizeof(szLog), "[DbProxy]%s[%d]load guarderId=%s, orgId=%s, state=%hu, roleType=%hu"
								"\r\n", __FUNCTION__, __LINE__, pGuarder->szId, pGuarder->szOrg, pGuarder->usState, 
								pGuarder->usRoleType);
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);

							i++;
						}
						if (pQryFrom_ && strlen(pQryFrom_)) {
							replyQuery(pGuarderList, (unsigned int)nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, 
								ulQryTime_, pQryFrom_);
						}
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query row=%u, seq=%u, time=%llu, from=%s, sql=%s\r\n", 
							__FUNCTION__, __LINE__, (unsigned int)nCount, uiQrySeq_, ulQryTime_, pQryFrom_ ? pQryFrom_ : " ", 
							pSqlStatement_->pStatement);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
							if (row[10] && strlen(row[10])) {
								strncpy_s(pSqlTaskList[i].szHandset, sizeof(pSqlTaskList[i].szHandset), row[10], strlen(row[10]));
							}
							EscortTask * pTask = (EscortTask *)zmalloc(nTaskSize);
							memset(pTask, 0, sizeof(EscortTask));
							strncpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), pSqlTaskList[i].szTaskId,
								strlen(pSqlTaskList[i].szTaskId));
							strncpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), pSqlTaskList[i].szDeviceId,
								strlen(pSqlTaskList[i].szDeviceId));
							strncpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), pSqlTaskList[i].szGuarderId,
								strlen(pSqlTaskList[i].szGuarderId));
							unsigned long long ulTaskStartTime = sqldatetime2time(pSqlTaskList[i].szStartTime);
							format_datetime(ulTaskStartTime, pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime));
							strncpy_s(pTask->szDestination, sizeof(pTask->szDestination), pSqlTaskList[i].szDestination,
								strlen(pSqlTaskList[i].szDestination));
							pTask->nTaskType = (uint8_t)pSqlTaskList[i].usTaskType;
							pTask->nTaskLimitDistance = (uint8_t)pSqlTaskList[i].usTaskLimit;
							sprintf_s(pTask->szTarget, sizeof(pTask->szTarget), "%s&%s", pSqlTaskList[i].person.szPersonId,
								pSqlTaskList[i].person.szPersonName);
							pTask->nTaskFlee = pSqlTaskList[i].nFleeFlag;
							if (strlen(pSqlTaskList[i].szHandset)) {
								strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), pSqlTaskList[i].szHandset,
									strlen(pSqlTaskList[i].szHandset));
							}
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
								strncpy_s(pTask->szOrg, sizeof(pTask->szOrg), pDev->deviceBasic.szOrgId, 
									strlen(pDev->deviceBasic.szOrgId));
							}
							pthread_mutex_unlock(&g_mutex4DevList);
							pthread_mutex_lock(&g_mutex4GuarderList);
							Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pSqlTaskList[i].szGuarderId);
							if (pGuarder) {
								pGuarder->usState = STATE_GUARDER_DUTY;
								strncpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), pSqlTaskList[i].szDeviceId,
									strlen(pSqlTaskList[i].szDeviceId));
								if (strlen(pTask->szOrg) == 0) {
									strncpy_s(pTask->szOrg, sizeof(pTask->szOrg), pGuarder->szOrg, strlen(pGuarder->szOrg));
								}
								strncpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), pSqlTaskList[i].szTaskId,
									strlen(pSqlTaskList[i].szTaskId));
							}
							pthread_mutex_unlock(&g_mutex4GuarderList);

							memcpy_s(&pTaskList[i], nTaskSize, pTask, nTaskSize);
							
							pthread_mutex_lock(&g_mutex4TaskList);
							zhash_update(g_taskList, pTask->szTaskId, pTask);
							zhash_freefn(g_taskList, pTask->szTaskId, free);
							pthread_mutex_unlock(&g_mutex4TaskList);

							sprintf_s(szLog, sizeof(szLog), "[DbProxy]%s[%d]load taskId=%s, deviceId=%s, factoryId=%s, orgId=%s, "
								"startTime=%s, handset=%s, flee=%d\r\n", __FUNCTION__, __LINE__, pTask->szTaskId, pTask->szFactoryId,
								pTask->szDeviceId, pTask->szOrg, pTask->szTaskStartTime, pTask->szHandset, (int)pTask->nTaskFlee);
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);

							i++;
						}
						if (pQryFrom_ && strlen(pQryFrom_)) {
							replyQuery(pTaskList, (unsigned int)nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, 
								ulQryTime_, pQryFrom_);
						}
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query row=%u, seq=%u, time=%llu, from=%s, sql=%s\r\n",
							 __FUNCTION__, __LINE__, (unsigned int)nCount, uiQrySeq_, ulQryTime_, pQryFrom_ ? pQryFrom_ : " ", 
							pSqlStatement_->pStatement);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
							replyQuery(pPersonList, (unsigned int)nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, 
								ulQryTime_, pQryFrom_);
						}
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query row=%u, seq=%u, time=%llu, from=%s, "
							"sql=%s\r\n", __FUNCTION__, __LINE__, (unsigned int)nCount, uiQrySeq_, ulQryTime_,
							pQryFrom_ ? pQryFrom_ : "", pSqlStatement_->pStatement);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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

							sprintf_s(szLog, sizeof(szLog), "[DbProxy]%s[%d]load orgId=%s, orgName=%s, parentId=%s\r\n",
								__FUNCTION__, __LINE__, pOrg->szOrgId, pOrg->szOrgName, pOrg->szParentOrgId);
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);

							i++;
						}
						if (pQryFrom_ && strlen(pQryFrom_)) {
							replyQuery(pOrgList, (unsigned int)nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_,
								ulQryTime_, pQryFrom_);
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
					else if (pSqlStatement_->uiCorrelativeTable == escort_db::E_TBL_FENCE) {
						escort_db::SqlFence * pSqlFenceList = (escort_db::SqlFence *)zmalloc(sizeof(escort_db::SqlFence)
							* nCount);
						size_t nFenceSize = sizeof(EscortFence);
						EscortFence * pFenceList = (EscortFence *)zmalloc(nFenceSize * nCount);
						int i = 0; 
						while (row = mysql_fetch_row(res_ptr)) {
							if (row[0]) {
								pSqlFenceList[i].nFenceId = atoi(row[0]);
							}
							if (row[1]) {
								pSqlFenceList[i].usFenceType = (unsigned short)atoi(row[1]);
							}
							if (row[2] && strlen(row[2])) {
								strncpy_s(pSqlFenceList[i].szFenceContent, sizeof(pSqlFenceList[i].szFenceContent),
									row[2], strlen(row[2]));
							}
							if (row[3]) {
								pSqlFenceList[i].nFenceActive = (uint8_t)atoi(row[3]);
							}
							if (row[4]) {
								pSqlFenceList[i].nCoordinate = (uint8_t)atoi(row[4]);
							}
							
							EscortFence * pFence = (EscortFence *)zmalloc(nFenceSize);
							sprintf_s(pFence->szFenceId, sizeof(pFence->szFenceId), "%d", pSqlFenceList[i].nFenceId);
							pFence->nFenceType = pSqlFenceList[i].usFenceType;
							pFence->nActiveFlag = pSqlFenceList[i].nFenceActive;
							pFence->nCoordinate = pSqlFenceList[i].nCoordinate;
							strncpy_s(pFence->szFenceContent, sizeof(pFence->szFenceContent), 
								pSqlFenceList[i].szFenceContent, strlen(pSqlFenceList[i].szFenceContent));

							memcpy_s(&pFenceList[i], nFenceSize, pFence, nFenceSize);

							pthread_mutex_lock(&g_mutex4FenceList);
							zhash_update(g_fenceList, pFence->szFenceId, pFence);
							zhash_freefn(g_fenceList, pFence->szFenceId, free);
							pthread_mutex_unlock(&g_mutex4FenceList);

							sprintf_s(szLog, sizeof(szLog), "[DbProxy]%s[%d]load fenceId=%s, fenceType=%d, coordinate=%d, "
								"activeFlag=%d, content=%s\r\n", __FUNCTION__, __LINE__, pFence->szFenceId, 
								(int)pFence->nFenceType, (int)pFence->nCoordinate, (int)pFence->nActiveFlag, 
								pFence->szFenceContent);
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);

							i++;
						}
						if (pQryFrom_ && strlen(pQryFrom_)) {
							replyQuery(pFenceList, i, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_, pQryFrom_);
						}
						if (pSqlFenceList) {
							free(pSqlFenceList);
							pSqlFenceList = NULL;
						}
						if (pFenceList) {
							free(pFenceList);
							pFenceList = NULL;
						}
					}
					else if (pSqlStatement_->uiCorrelativeTable == escort_db::E_TBL_TASK_FENCE) {
						escort_db::SqlFenceTask * pSqlFenceTaskList = (escort_db::SqlFenceTask *)zmalloc(
							sizeof(escort_db::SqlFenceTask) * nCount);
						size_t nFenceTaskSize = sizeof(EscortFenceTask);
						EscortFenceTask * pFenceTaskList = (EscortFenceTask *)zmalloc(nFenceTaskSize * nCount);
						int i = 0;
						while (row = mysql_fetch_row(res_ptr)) {
							if (row[0]) {
								pSqlFenceTaskList[i].nFenceTaskId = atoi(row[0]);
							}
							if (row[1]) {
								pSqlFenceTaskList[i].nFenceId = atoi(row[1]);
							}
							if (row[2] && strlen(row[2])) { //factoryId
								strncpy_s(pSqlFenceTaskList[i].szFactoryId, sizeof(pSqlFenceTaskList[i].szFactoryId),
									row[2], strlen(row[2]));
							}
							if (row[3] && strlen(row[3])) { //deviceId
								strncpy_s(pSqlFenceTaskList[i].szDeviceId, sizeof(pSqlFenceTaskList[i].szDeviceId),
									row[3], strlen(row[3]));
							}
							if (row[4] && strlen(row[4])) { //startTime
								strncpy_s(pSqlFenceTaskList[i].szTaskStartTime, sizeof(pSqlFenceTaskList[i].szTaskStartTime),
									row[4], strlen(row[4]));
							}
							if (row[5] && strlen(row[5])) { //stopTime
								strncpy_s(pSqlFenceTaskList[i].szTaskStopTime, sizeof(pSqlFenceTaskList[i].szTaskStopTime),
									row[5], strlen(row[5]));
							}
							if (row[6] && strlen(row[6])) {
								pSqlFenceTaskList[i].nFencePolicy = (int8_t)atoi(row[6]);
							}
							if (row[7] && strlen(row[7])) {
								pSqlFenceTaskList[i].nPeerCheck = (int8_t)atoi(row[7]);
							}

							EscortFenceTask * pFenceTask = (EscortFenceTask *)zmalloc(nFenceTaskSize);
							sprintf_s(pFenceTask->szFenceTaskId, sizeof(pFenceTask->szFenceTaskId), "%d", 
								pSqlFenceTaskList[i].nFenceTaskId);
							sprintf_s(pFenceTask->szFenceId, sizeof(pFenceTask->szFenceId), "%d", pSqlFenceTaskList[i].nFenceId);
							strncpy_s(pFenceTask->szFactoryId, sizeof(pFenceTask->szFactoryId),
								pSqlFenceTaskList[i].szFactoryId, strlen(pSqlFenceTaskList[i].szFactoryId));
							strncpy_s(pFenceTask->szDeviceId, sizeof(pFenceTask->szDeviceId),
								pSqlFenceTaskList[i].szDeviceId, strlen(pSqlFenceTaskList[i].szDeviceId));
							unsigned long long ulStartTime = sqldatetime2time(pSqlFenceTaskList[i].szTaskStartTime);
							format_datetime(ulStartTime, pFenceTask->szStartTime, sizeof(pFenceTask->szStartTime));
							unsigned long long ulStopTime = sqldatetime2time(pSqlFenceTaskList[i].szTaskStopTime);
							format_datetime(ulStopTime, pFenceTask->szStopTime, sizeof(pFenceTask->szStopTime));
							pFenceTask->nFencePolicy = pSqlFenceTaskList[i].nFencePolicy;
							pFenceTask->nPeerCheck = pSqlFenceTaskList[i].nPeerCheck;
							pFenceTask->nTaskState = pSqlFenceTaskList[i].nTaskState;

							memcpy_s(&pFenceTaskList[i], nFenceTaskSize, pFenceTask, nFenceTaskSize);

							pthread_mutex_lock(&g_mutex4FenceTaskList);
							zhash_update(g_fenceTaskList, pFenceTask->szFenceTaskId, pFenceTask);
							zhash_freefn(g_fenceTaskList, pFenceTask->szFenceTaskId, free);
							pthread_mutex_unlock(&g_mutex4FenceTaskList);

							pthread_mutex_lock(&g_mutex4DevList);
							if (zhash_size(g_deviceList)) {
								WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pFenceTask->szDeviceId);
								if (pDevice) {
									pDevice->nDeviceHasFence = 1;
								}
							}
							pthread_mutex_unlock(&g_mutex4DevList);

							sprintf_s(szLog, sizeof(szLog), "[DbProxy]%s[%d]load fenceTaskId=%s, fenceId=%s, deviceId=%s, "
								"factoryId=%s, state=%d, policy=%d, startTime=%s, stopTime=%s, peerCheck=%d\r\n", __FUNCTION__,
								__LINE__, pFenceTask->szFenceTaskId, pFenceTask->szFenceId, pFenceTask->szFactoryId, 
								pFenceTask->szDeviceId, pFenceTask->nTaskState, pFenceTask->nFencePolicy, pFenceTask->szStartTime,
								pFenceTask->szStopTime, pFenceTask->nPeerCheck);
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);

							i++;
						}
						if (pQryFrom_ && strlen(pQryFrom_)) {
							replyQuery(pFenceTaskList, (unsigned int)nCount, pSqlStatement_->uiCorrelativeTable, uiQrySeq_,
								ulQryTime_, pQryFrom_);
						}
						if (pSqlFenceTaskList) {
							free(pSqlFenceTaskList);
							pSqlFenceTaskList = NULL;
						}
						if (pFenceTaskList) {
							free(pFenceTaskList);
							pFenceTaskList = NULL;
						}
					}
				}
				else { //rowCount = 0;
					if (pQryFrom_ && strlen(pQryFrom_)) {
						replyQuery(NULL, 0, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_, pQryFrom_);
					}
					snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query row is empty, seq=%u, time=%llu, from=%s"
						", sql=%s\r\n", __FUNCTION__, __LINE__, uiQrySeq_, ulQryTime_,
						pQryFrom_ ? pQryFrom_ : "", pSqlStatement_->pStatement);
					LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				}
				mysql_free_result(res_ptr);
			}
			else { //res_ptr query null
				if (pQryFrom_ && strlen(pQryFrom_)) {
					replyQuery(NULL, 0, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_, pQryFrom_);
				}
				snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]get store error=%d,%s, seq=%u, time=%llu, from=%s, "
					"sql=%s\r\n", __FUNCTION__, __LINE__, mysql_errno(m_readConn), mysql_error(m_readConn),
					uiQrySeq_, ulQryTime_, pQryFrom_, pSqlStatement_->pStatement);
				LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
			}
		}
		else { //query error
			if (pQryFrom_ && strlen(pQryFrom_)) {
				replyQuery(NULL, 0, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_, pQryFrom_);
			}
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]query error=%u,%s, seq=%u, time=%llu, from=%s, sql="
				"%s\r\n", __FUNCTION__, __LINE__, mysql_errno(m_readConn), mysql_error(m_readConn), uiQrySeq_, 
				ulQryTime_, pQryFrom_ ? pQryFrom_ : "", pSqlStatement_->pStatement);
			LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
		}
	}
	else { //conn error
		if (pQryFrom_ && strlen(pQryFrom_)) {
			replyQuery(NULL, 0, pSqlStatement_->uiCorrelativeTable, uiQrySeq_, ulQryTime_, pQryFrom_);
		}
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]conn error, seq=%u, time=%llu, from=%s, sql=%s\r\n",
			__FUNCTION__, __LINE__, uiQrySeq_, ulQryTime_, pQryFrom_ ? pQryFrom_ : "", 
			(pSqlStatement_ && pSqlStatement_->pStatement) ? pSqlStatement_->pStatement : "");
		LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
	}
	pthread_mutex_unlock(&m_mutex4ReadConn);
}

void DbProxy::handleSqlExec(dbproxy::SqlStatement * pSqlStatement_, unsigned int uiExecSeq_, 
	unsigned long long ulExecTime_, const char * pExecFrom_)
{
	char szLog[1024] = { 0 };
	pthread_mutex_lock(&m_mutex4WriteConn);
	if (m_writeConn && pSqlStatement_ && pSqlStatement_->pStatement && pSqlStatement_->uiStatementLen) {
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]seq=%u, time=%llu, execute sql=%s\r\n",
			__FUNCTION__, __LINE__, uiExecSeq_, ulExecTime_, pSqlStatement_->pStatement);
		//LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		printf(szLog);
		int nErr = mysql_real_query(m_writeConn, pSqlStatement_->pStatement, pSqlStatement_->uiStatementLen);
		if (nErr == 0) {
			my_ulonglong nAffectedRow = mysql_affected_rows(m_writeConn);
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute sql affected=%d, seq=%u, time=%llu, from=%s,"
				"sql=%s\r\n", __FUNCTION__, __LINE__, (int)nAffectedRow, uiExecSeq_, ulExecTime_,
				pExecFrom_ ? pExecFrom_ : "", pSqlStatement_->pStatement);
			LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
		else {
			//need backup
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute sql error=%d, %s, seq=%u, time=%llu, from=%s, "
				"sql=%s\r\n", __FUNCTION__, __LINE__, mysql_errno(m_writeConn), mysql_error(m_writeConn), uiExecSeq_, 
				ulExecTime_, pExecFrom_ ? pExecFrom_ : "", pSqlStatement_->pStatement);
			LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (nErr == 2006 || nErr == 2013) {
				mysql_close(m_writeConn);
				m_writeConn = mysql_init(NULL);
				if (m_writeConn && mysql_real_connect(m_writeConn, m_zkDbProxy.szDbHostIp, m_zkDbProxy.szDbUser,
					m_zkDbProxy.szDbPasswd, m_zkDbProxy.szMajorSample, m_zkDbProxy.usMasterDbPort, NULL, 0)) {
					snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect write db %s, ip=%s, port=%hu, user=%s at %llu\r\n",
						__FUNCTION__, __LINE__, m_zkDbProxy.szMajorSample, m_zkDbProxy.szDbHostIp, m_zkDbProxy.usMasterDbPort,
						m_zkDbProxy.szDbUser, (unsigned long long)time(NULL));
					LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					mysql_set_character_set(m_writeConn, "gb2312");
				}
			}
			int nRetryTime = 2;
			while (nRetryTime > 0) {
				int nErr2 = mysql_real_query(m_writeConn, pSqlStatement_->pStatement, pSqlStatement_->uiStatementLen);
				if (nErr2 == 0) {
					my_ulonglong nAffectedRow = mysql_affected_rows(m_writeConn);
					snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]retry=%d, execute sql affected=%d, seq=%u, time=%llu, "
						"from=%s, sql=%s\r\n", __FUNCTION__, __LINE__, nRetryTime, (int)nAffectedRow, uiExecSeq_, ulExecTime_,
						pExecFrom_ ? pExecFrom_ : "", pSqlStatement_->pStatement);
					LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					mysql_set_character_set(m_writeConn, "gb2312");
					break;
				}
				snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]retry=%d, execute sql error=%d, %s, seq=%u, time=%llu, "
					"from=%s, sql=%s\r\n", __FUNCTION__, __LINE__, nRetryTime, mysql_errno(m_writeConn), 
					mysql_error(m_writeConn), uiExecSeq_, ulExecTime_, pExecFrom_ ? pExecFrom_ : "", 
					pSqlStatement_->pStatement);
				LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				nRetryTime--;
				Sleep(200);
			}
		}
	}
	else {
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]write conn error, seq=%u, time=%llu, from=%s, sql=%s\r\n",
			__FUNCTION__, __LINE__, uiExecSeq_, ulExecTime_, pExecFrom_ ? pExecFrom_ : "",
			(pSqlStatement_ && pSqlStatement_->pStatement) ? pSqlStatement_->pStatement : "");
		LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
	}
	pthread_mutex_unlock(&m_mutex4WriteConn);
}

void DbProxy::handleSqlLocate(dbproxy::SqlStatement * pSqlStatement_, unsigned int uiLocateSeq_, 
	unsigned long long ulLocateTime_)
{
	char szLog[512] = { 0 };
	pthread_mutex_lock(&m_mutex4LocateConn);
	if (m_locateConn && pSqlStatement_ && pSqlStatement_->pStatement && pSqlStatement_->uiStatementLen) {
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]seq=%u, time=%llu, execute locate sql=%s\r\n",
			__FUNCTION__, __LINE__, uiLocateSeq_, ulLocateTime_, pSqlStatement_->pStatement);
		//LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		printf(szLog);
		int nErr = mysql_real_query(m_locateConn, pSqlStatement_->pStatement, pSqlStatement_->uiStatementLen);
		if (nErr == 0) {
			my_ulonglong nAffectedRow = mysql_affected_rows(m_writeConn);
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute sql affected=%d, seq=%u, time=%llu, sql=%s\r\n",
				__FUNCTION__, __LINE__, (int)nAffectedRow, uiLocateSeq_, ulLocateTime_, pSqlStatement_->pStatement);
			LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
		}
		else {
			//backup
			snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]locate conn error=%d, seq=%u, time=%llu, sql=%s\r\n",
				__FUNCTION__, __LINE__, mysql_errno(m_locateConn), uiLocateSeq_, ulLocateTime_,
				pSqlStatement_->pStatement);
			LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
			if (nErr == 2006) {
				mysql_close(m_locateConn);
				m_locateConn = mysql_init(NULL);
				if (m_locateConn && mysql_real_connect(m_locateConn, m_zkDbProxy.szDbHostIp, m_zkDbProxy.szDbUser,
					m_zkDbProxy.szDbPasswd, m_zkDbProxy.szLocateSample, m_zkDbProxy.usMasterDbPort, NULL, 0)) {
					snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect locate db %s, ip=%s, port=%hu, user=%s at %llu\r\n",
						__FUNCTION__, __LINE__, m_zkDbProxy.szMajorSample, m_zkDbProxy.szDbHostIp, m_zkDbProxy.usMasterDbPort, 
						m_zkDbProxy.szDbUser, (unsigned long long)time(NULL));
					LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					mysql_set_character_set(m_locateConn, "gb2312");
				}
			}
			int nRetryTime = 2;
			while (nRetryTime > 0) {
				int nErr2 = mysql_real_query(m_locateConn, pSqlStatement_->pStatement, pSqlStatement_->uiStatementLen);
				if (nErr2 == 0) {
					my_ulonglong nAffectedRow = mysql_affected_rows(m_writeConn);
					snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]retry=%d execute sql affected=%d, seq=%u, time=%llu, "
						"sql=%s\r\n", __FUNCTION__, __LINE__, nRetryTime, (int)nAffectedRow, uiLocateSeq_, ulLocateTime_, 
						pSqlStatement_->pStatement);
					LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					break;
				}
				snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]retry=%d, locate conn error=%d, %s, seq=%u, time=%llu, "
					"sql=%s\r\n", __FUNCTION__, __LINE__, nRetryTime, mysql_errno(m_locateConn), mysql_error(m_locateConn),
					uiLocateSeq_, ulLocateTime_, pSqlStatement_->pStatement);
				LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
				nRetryTime--;
				Sleep(200);
			} 
		}
	}
	else {
		snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]locate conn error, seq=%u, time=%llu, sql=%s\r\n",
			__FUNCTION__, __LINE__, uiLocateSeq_, ulLocateTime_, 
			(pSqlStatement_ && pSqlStatement_->pStatement) ? pSqlStatement_->pStatement : "");
		LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
	}
	pthread_mutex_unlock(&m_mutex4LocateConn);
}

void DbProxy::replyQuery(void * pReplyData_, unsigned int uiReplyCount_, unsigned int uiCorrelativeTable_,
	unsigned int uiQrySeq_, unsigned long long ulQryTime_, const char * pQryFrom_)
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
				case escort_db::E_TBL_FENCE: {
					EscortFence * pFenceList = (EscortFence *)pReplyData_;
					container.uiResultLen = sizeof(EscortFence) * uiReplyCount_;
					container.pStoreResult = (unsigned char *)zmalloc(container.uiResultLen + 1);
					memcpy_s(container.pStoreResult, container.uiResultLen, pFenceList, container.uiResultLen);
					container.pStoreResult[container.uiResultLen] = '\0';
					break;
				}
				case escort_db::E_TBL_TASK_FENCE: {
					EscortFenceTask * pFenceTaskList = (EscortFenceTask *)pReplyData_;
					container.uiResultLen = sizeof(EscortFenceTask) * uiReplyCount_;
					container.pStoreResult = (unsigned char *)zmalloc(container.uiResultLen + 1);
					memcpy_s(container.pStoreResult, container.uiResultLen, pFenceTaskList, container.uiResultLen);
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
		int rc = zmq_poll(items, 4, 500 * ZMQ_POLL_MSEC);
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
				zframe_t * frame_from = zmsg_pop(subMsg);
				char szMark[64] = { 0 };
				memcpy_s(szMark, sizeof(szMark), zframe_data(frame_mark), zframe_size(frame_mark));
				char szSeq[20] = { 0 };
				memcpy_s(szSeq, sizeof(szSeq), zframe_data(frame_seq), zframe_size(frame_seq));
				char szType[16] = { 0 };
				memcpy_s(szType, sizeof(szType), zframe_data(frame_type), zframe_size(frame_type));
				char szUuid[64] = { 0 };
				memcpy_s(szUuid, sizeof(szUuid), zframe_data(frame_uuid), zframe_size(frame_uuid));
				char szBody[512] = { 0 };
				memcpy_s(szBody, sizeof(szBody), zframe_data(frame_body), zframe_size(frame_body));
				char szFrom[64] = { 0 };
				memcpy_s(szFrom, sizeof(szFrom), zframe_data(frame_from), zframe_size(frame_from));
				TopicMessage * pMsg = (TopicMessage *)zmalloc(sizeof(TopicMessage));
				if (pMsg) {
					strncpy_s(pMsg->szMsgMark, sizeof(pMsg->szMsgMark), szMark, strlen(szMark));
					strncpy_s(pMsg->szMsgUuid, sizeof(pMsg->szMsgUuid), szUuid, strlen(szUuid));
					strncpy_s(pMsg->szMsgBody, sizeof(pMsg->szMsgBody), szBody, strlen(szBody));
					strncpy_s(pMsg->szMsgFrom, sizeof(pMsg->szMsgFrom), szFrom, strlen(szFrom));
					pMsg->uiMsgSequence = (unsigned int)atoi(szSeq);
					pMsg->uiMsgType = (unsigned int)atoi(szType);
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
				zframe_destroy(&frame_from);
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
					pMsg->uiContentCount = (unsigned int)nCount;
					pMsg->pMsgContents = (char **)zmalloc(nCount * sizeof(char *));
					pMsg->uiContentLens = (unsigned int *)zmalloc(nCount * sizeof(unsigned int));
					for (size_t i = 0; i < nCount; i++) {
						interactFrames[i] = zmsg_pop(interactMsg);
						size_t nFrameLen = zframe_size(interactFrames[i]);
						pMsg->uiContentLens[i] = (unsigned int)nFrameLen;
						pMsg->pMsgContents[i] = (char *)zmalloc(nFrameLen + 1);
						memcpy_s(pMsg->pMsgContents[i], nFrameLen + 1, zframe_data(interactFrames[i]), nFrameLen);
						pMsg->pMsgContents[i][nFrameLen] = '\0';
						zframe_destroy(&interactFrames[i]);
					}
					if (!addInteractMsg(pMsg)) {
						for (size_t i = 0; i < nCount; i++) {
							if (pMsg->pMsgContents[i]) {
								free(pMsg->pMsgContents[i]);
								pMsg->pMsgContents[i] = NULL;
							}
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
				char szIdentity[64] = { 0 };
				if (zframe_size(frame_identity)) {
					memcpy_s(szIdentity, sizeof(szIdentity), zframe_data(frame_identity), zframe_size(frame_identity));
				}
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
						memcpy_s(container.pStoreResult, container.uiResultLen, pFrameData + nContainerSize, container.uiResultLen);
						container.pStoreResult[container.uiResultLen] = '\0';
					}
					handleReception(&container, szIdentity);
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
			switch (pMsg->uiMsgType) {
				case PUBMSG_DEVICE_ALIVE: {
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
							handleTopicDeviceAliveMsg(&aliveMsg);
							storeTopicMsg(pMsg, aliveMsg.ulMessageTime);
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_ALIVE_MSG data miss, uuid=%s, seq=%u"
								", factoryId=%s, deviceId=%s, orgId=%s, battery=%u, datetime=%s\r\n",
								__FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence, aliveMsg.szFactoryId,
								aliveMsg.szDeviceId, aliveMsg.szOrg, aliveMsg.usBattery, 
								bValidDatetime ? szDatetime : "null");
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProx]%s[%d]parse MSG_DEVICE_ALIVE msg data uuid=%s"
							", seq=%u, parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, 
							pMsg->uiMsgSequence);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_ONLINE: {
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
							handleTopicDeviceOnlineMsg(&onlineMsg);
							storeTopicMsg(pMsg, onlineMsg.ulMessageTime);
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_ALIVE_MSG data miss, uuid=%s, seq=%u,"
								" factoryId=%s, deviceId=%s, orgId=%s, battery=%u, datetime=%s\r\n",
								__FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence, 
								bValidFactory ? onlineMsg.szFactoryId : "null", onlineMsg.szDeviceId, onlineMsg.szOrg,
								onlineMsg.usBattery, bValidDatetime ? szDatetime : "null");
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_ONLINE msg data uuid=%s, "
							"seq=%u, parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid,
							pMsg->uiMsgSequence);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_OFFLINE: {
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
							handleTopicDeviceOfflineMsg(&offlineMsg);
							storeTopicMsg(pMsg, offlineMsg.ulMessageTime);
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_DEVICE_OFFLINE data miss, uuid=%s, seq=%u, factoryId=%s,"
								" deviceId=%s, datetime=%s\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence, 
								offlineMsg.szFactoryId, offlineMsg.szDeviceId, szDatetime);
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_OFFLINE msg data, uuid=%s, seq=%u, "
							"parse JSON data error\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_LOCATE: {
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
										gpsLocateMsg.nFlag = doc["locateFlag"].GetInt();
										bValidFlag = true;
									}
								}
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										gpsLocateMsg.nCoordinate = doc["coordinate"].GetInt();
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
								if (doc.HasMember("sattelite")) {
									if (doc["sattelite"].IsInt()) {
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
									if (doc["direction"].IsDouble()) {
										gpsLocateMsg.dDirection = doc["direction"].GetDouble();
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
										"%d, sattelite=%u, intensity=%u, speed=%.04f, direction=%.04f, battery=%u, datetime=%s, "
										"coordinate=%d\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence,
										bValidFactory ? gpsLocateMsg.szFactoryId : "null",
										bValidDevice ? gpsLocateMsg.szDeviceId : "null",
										gpsLocateMsg.szOrg, gpsLocateMsg.dLat, gpsLocateMsg.usLatType, gpsLocateMsg.dLng,
										gpsLocateMsg.usLngType, gpsLocateMsg.nFlag, gpsLocateMsg.usStattelite,
										gpsLocateMsg.usIntensity, gpsLocateMsg.dSpeed, gpsLocateMsg.dDirection,
										gpsLocateMsg.usBattery, bValidDatetime ? szDatetime : "null",
										gpsLocateMsg.nCoordinate);
									LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										lbsLocateMsg.nCoordinate = doc["coordinate"].GetInt();
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
										 " precision=%d, battery=%u, datetime=%s, coordinate=%d\r\n", __FUNCTION__, __LINE__, 
										pMsg->szMsgUuid, pMsg->uiMsgSequence, lbsLocateMsg.usFlag, 
										bValidFactory ? lbsLocateMsg.szFactoryId : "null",
										bValidDevice ? lbsLocateMsg.szDeviceId : "null", lbsLocateMsg.szOrg, lbsLocateMsg.dLat,
										lbsLocateMsg.usLatType, lbsLocateMsg.dLng, lbsLocateMsg.usLngType, lbsLocateMsg.nPrecision,
										lbsLocateMsg.usBattery, bValidDatetime ? szDatetime : "null", lbsLocateMsg.nCoordinate);
									LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case LOCATE_APP: {
								TopicLocateMessageApp appLocateMsg;
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
											strncpy_s(appLocateMsg.szFactoryId, sizeof(appLocateMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
											bValidFactory = true;
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(appLocateMsg.szDeviceId, sizeof(appLocateMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
											bValidDevice = true;
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(appLocateMsg.szOrg, sizeof(appLocateMsg.szOrg),
												doc["orgId"].GetString(), nSize);
											bValidOrg = true;
										}
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										double d = doc["latitude"].GetDouble();
										if (d > 0) {
											appLocateMsg.dLat = d;
											bValidLat = true;
										}
									}
								}
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										double d = doc["lngitude"].GetDouble();
										if (d > 0) {
											appLocateMsg.dLng = d;
											bValidLng = true;
										}
									}
								}
								if (doc.HasMember("taskId")) {
									if (doc["taskId"].IsString()) {
										size_t nSize = doc["taskId"].GetStringLength();
										if (nSize) {
											strncpy_s(appLocateMsg.szTaskId, sizeof(appLocateMsg.szTaskId),
												doc["taskId"].GetString(), nSize);
											bValidTask = true;
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										appLocateMsg.usBattery = (unsigned short)doc["battery"].GetInt();
										bValidBattery = true;
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											appLocateMsg.ulMessageTime = strdatetime2time(szDatetime);
											bValidDatetime = true;
										}
									}
								}
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										appLocateMsg.nCoordinate = (signed short)doc["coordinate"].GetInt();
									}
								}
								if (bValidFactory && bValidDevice && bValidOrg && bValidTask && bValidLat && bValidLng
									&& bValidBattery && bValidDatetime) {
									handleTopicAppLocateMsg(&appLocateMsg);
									storeTopicMsg(pMsg, appLocateMsg.ulMessageTime);
								}
								else {
									snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_DEVICE_LOCATE msg data miss, uuid=%s,"
										" seq=%u, factoryId=%s, deviceId=%s, taskId=%s, orgId=%s, lat=%.06f, lng=%.06f, "
										"datetime=%s, coordinate=%d\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, 
										pMsg->uiMsgSequence, bValidFactory ? appLocateMsg.szFactoryId : "null",
										bValidDevice ? appLocateMsg.szDeviceId : "null",
										bValidTask ? appLocateMsg.szTaskId : "null", bValidOrg ? appLocateMsg.szOrg : "null",
										appLocateMsg.dLat, appLocateMsg.dLng, bValidDatetime ? szDatetime : "null", 
										appLocateMsg.nCoordinate);
									LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_LOCATE msg data, subType="
									"%d, uuid=%s, seq=%u, error\r\n", __FUNCTION__, __LINE__, nSubType, pMsg->szMsgUuid,
									pMsg->uiMsgSequence);
								LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_LOCATE msg data, uuid=%s, seq=%u"
							", parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_ALARM: {
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
										__FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence,
										bValidFactory ? lowpowerAlarmMsg.szFactoryId : "null",
										bValidDevice ? lowpowerAlarmMsg.szDeviceId : "null", lowpowerAlarmMsg.szOrg,
										lowpowerAlarmMsg.usMode, lowpowerAlarmMsg.usBattery,
										bValidDatetime ? szDatetime : "null");
									LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
										__FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence,
										bValidFactory ? looseAlarmMsg.szFactoryId : "null",
										bValidDevice ? looseAlarmMsg.szDeviceId : "null", looseAlarmMsg.szOrg,
										looseAlarmMsg.usMode, looseAlarmMsg.usBattery, bValidDatetime ? szDatetime : "null");
									LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
										pMsg->uiMsgSequence, bValidFactory ? fleeAlarmMsg.szFactoryId : "null",
										bValidDevice ? fleeAlarmMsg.szDeviceId : "null",
										bValidGuarder ? fleeAlarmMsg.szGuarder : "null",
										bValidTask ? fleeAlarmMsg.szTaskId : "null",
										bValidOrg ? fleeAlarmMsg.szOrg : "null", bValidMode ? fleeAlarmMsg.usMode : -1,
										fleeAlarmMsg.usBattery, bValidDatetime ? szDatetime : "null");
									LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								}
								break;
							}
							case ALARM_DEVICE_FENCE: {
								TopicAlarmMessageFence fenceAlarmMsg;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strncpy_s(fenceAlarmMsg.szFactoryId, sizeof(fenceAlarmMsg.szFactoryId),
												doc["factoryId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strncpy_s(fenceAlarmMsg.szDeviceId, sizeof(fenceAlarmMsg.szDeviceId),
												doc["deviceId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strncpy_s(fenceAlarmMsg.szOrgId, sizeof(fenceAlarmMsg.szOrgId),
												doc["orgId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("fenceId")) {
									if (doc["fenceId"].IsString()) {
										size_t nSize = doc["fenceId"].GetStringLength();
										if (nSize) {
											strncpy_s(fenceAlarmMsg.szFenceId, sizeof(fenceAlarmMsg.szFenceId),
												doc["fenceId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("fenceTaskId")) {
									if (doc["fenceTaskId"].IsString()) {
										size_t nSize = doc["fenceTaskId"].GetStringLength();
										if (nSize) {
											strncpy_s(fenceAlarmMsg.szFenceTaskId, sizeof(fenceAlarmMsg.szFenceTaskId),
												doc["fenceTaskId"].GetString(), nSize);
										}
									}
								}
								if (doc.HasMember("latitude")) {
									if (doc["latitude"].IsDouble()) {
										fenceAlarmMsg.dLatitude = doc["latitude"].GetDouble();
									}
								}
								if (doc.HasMember("latType")) {
									if (doc["latType"].IsInt()) {
										fenceAlarmMsg.usLatType = (unsigned short)doc["latType"].GetInt();
									}
								}
								if (doc.HasMember("lngitude")) {
									if (doc["lngitude"].IsDouble()) {
										fenceAlarmMsg.dLngitude = doc["lngitude"].GetDouble();
									}
								}
								if (doc.HasMember("lngType")) {
									if (doc["lngType"].IsInt()) {
										fenceAlarmMsg.usLngType = (unsigned short)doc["lngType"].GetInt();
									}
								}
								if (doc.HasMember("locateType")) {
									if (doc["locateType"].IsInt()) {
										fenceAlarmMsg.nLocateType = (int8_t)doc["locateType"].GetInt();
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										fenceAlarmMsg.nMode = (int8_t)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
											fenceAlarmMsg.ulMessageTime = strdatetime2time(szDatetime);
										}
									}
								}
								if (doc.HasMember("coordinate")) {
									if (doc["coordinate"].IsInt()) {
										fenceAlarmMsg.nCoordinate = (int8_t)doc["coordinate"].GetInt();
									}
								}
								if (doc.HasMember("policy")) {
									if (doc["policy"].IsInt()) {
										fenceAlarmMsg.nPolicy = (int8_t)doc["policy"].GetInt();
									}
								}
								if (strlen(fenceAlarmMsg.szFenceTaskId) && strlen(fenceAlarmMsg.szFenceId)
									&& strlen(fenceAlarmMsg.szDeviceId) && strlen(fenceAlarmMsg.szFactoryId)
									&& strlen(fenceAlarmMsg.szOrgId) && fenceAlarmMsg.dLatitude > 0.00
									&& fenceAlarmMsg.dLngitude > 0.00 && fenceAlarmMsg.ulMessageTime > 0) {
									handleTopicFenceAlarmMsg(&fenceAlarmMsg);
									storeTopicMsg(pMsg, fenceAlarmMsg.ulMessageTime);
								}
								break;
							}
							case ALARM_LOCATE_LOST: {
								TopicAlarmMessageLocateLost alarmMsg;
								char szDatetime[20] = { 0 };
								if (doc.HasMember("factoryId")) {
									if (doc["factoryId"].IsString()) {
										size_t nSize = doc["factoryId"].GetStringLength();
										if (nSize) {
											strcpy_s(alarmMsg.szFactoryId, sizeof(alarmMsg.szFactoryId), doc["factoryId"].GetString());
										}
									}
								}
								if (doc.HasMember("deviceId")) {
									if (doc["deviceId"].IsString()) {
										size_t nSize = doc["deviceId"].GetStringLength();
										if (nSize) {
											strcpy_s(alarmMsg.szDeviceId, sizeof(alarmMsg.szDeviceId), doc["deviceId"].GetString());
										}
									}
								}
								if (doc.HasMember("orgId")) {
									if (doc["orgId"].IsString()) {
										size_t nSize = doc["orgId"].GetStringLength();
										if (nSize) {
											strcpy_s(alarmMsg.szOrg, sizeof(alarmMsg.szOrg), doc["orgId"].GetString());
										}
									}
								}
								if (doc.HasMember("guarder")) {
									if (doc["guarder"].IsString()) {
										size_t nSize = doc["guarder"].GetStringLength();
										if (nSize) {
											strcpy_s(alarmMsg.szGuarder, sizeof(alarmMsg.szGuarder), doc["guarder"].GetString());
										}
									}
								}
								if (doc.HasMember("battery")) {
									if (doc["battery"].IsInt()) {
										alarmMsg.usDeviceBattery = (unsigned short)doc["battery"].GetInt();
									}
								}
								if (doc.HasMember("mode")) {
									if (doc["mode"].IsInt()) {
										alarmMsg.usAlarmMode = (unsigned short)doc["mode"].GetInt();
									}
								}
								if (doc.HasMember("datetime")) {
									if (doc["datetime"].IsString()) {
										size_t nSize = doc["datetime"].GetStringLength();
										if (nSize) {
											strcpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString());
											alarmMsg.ulMessageTime = strdatetime2time(szDatetime);
										}
									}
								}
								if (strlen(alarmMsg.szDeviceId) && strlen(alarmMsg.szGuarder) && strlen(alarmMsg.szOrg)
									&& strlen(szDatetime)) {
									handleTopicLocateLostAlarmMsg(&alarmMsg);
								}
								else {
									sprintf_s(szLog, sizeof(szLog), "[dbproxy]%s[%d]alarm locate lost message, data parameter miss,"
										" deviceId=%s, factoryId=%s, orgId=%s, guarder=%s, battery=%hu, datetime=%s\r\n", __FUNCTION__,
										__LINE__, alarmMsg.szDeviceId, alarmMsg.szFactoryId, alarmMsg.szOrg, alarmMsg.szGuarder,
										alarmMsg.usDeviceBattery, szDatetime);
									LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								}
								break;
							}
							default: {
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_DEVICE_ALARM unsupport type=%d, uuid=%s,"
									" seq=%u\r\n", __FUNCTION__, __LINE__, nSubType, pMsg->szMsgUuid, pMsg->uiMsgSequence);
								LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_ALARM msg data, uuid=%s, seq=%u"
							", parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_DEVICE_BIND: {
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
									strcpy_s(bindMsg.szFactoryId, sizeof(bindMsg.szFactoryId), doc["factoryId"].GetString());
									bValidFactory = true;
								}
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString()) {
								size_t nSize = doc["deviceId"].GetStringLength();
								if (nSize) {
									strcpy_s(bindMsg.szDeviceId, sizeof(bindMsg.szDeviceId), doc["deviceId"].GetString());
									bValidDevice = true;
								}
							}
						}
						if (doc.HasMember("orgId")) {
							if (doc["orgId"].IsString()) {
								size_t nSize = doc["orgId"].GetStringLength();
								if (nSize) {
									strcpy_s(bindMsg.szOrg, sizeof(bindMsg.szOrg), doc["orgId"].GetString());
								}
							}
						}
						if (doc.HasMember("guarder")) {
							if (doc["guarder"].IsString()) {
								size_t nSize = doc["guarder"].GetStringLength();
								if (nSize) {
									strcpy_s(bindMsg.szGuarder, sizeof(bindMsg.szGuarder), doc["guarder"].GetString());
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
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_DEVICE_BIND data miss, uuid=%s, seq=%u, factoryId=%s, "
								"deviceId=%s, orgId=%s, guarder=%s, mode=%d, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, 
								pMsg->uiMsgSequence, bindMsg.szFactoryId, bindMsg.szDeviceId, bindMsg.szOrg, bindMsg.szGuarder, 
								bindMsg.usMode, szDatetime);
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_DEVICE_BIND msg data, uuid=%s, seq=%u, "
							"parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_TASK: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						int nSubType = -1;
						if (doc.HasMember("subType")) {
							if (doc["subType"].IsInt()) {
								nSubType = doc["subType"].GetInt();
							}
						}
						if (nSubType == TASK_OPT_SUBMIT) {
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
											doc["destination"].GetString(),
											sizeof(taskMsg.szDestination) >= nSize ? nSize : sizeof(taskMsg.szDestination));
									}
								}
							}
							if (doc.HasMember("target")) {
								if (doc["target"].IsString()) {
									size_t nSize = doc["target"].GetStringLength();
									if (nSize) {
										strncpy_s(taskMsg.szTarget, sizeof(taskMsg.szTarget),
											doc["target"].GetString(), 
											sizeof(taskMsg.szTarget) >= nSize ? nSize : sizeof(taskMsg.szDestination));
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
							if (doc.HasMember("handset")) {
								if (doc["handset"].IsString()) {
									size_t nSize = doc["handset"].GetStringLength();
									if (nSize) {
										strncpy_s(taskMsg.szHandset, sizeof(taskMsg.szHandset), doc["handset"].GetString(), nSize);
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
									pMsg->uiMsgSequence, bValidTask ? taskMsg.szTaskId : "null",
									bValidFactory ? taskMsg.szFactoryId : "null", bValidDevice ? taskMsg.szDeviceId : "null",
									bValidOrg ? taskMsg.szOrg : "null", bValidGuarder ? taskMsg.szGuarder : "null",
									taskMsg.usTaskType, taskMsg.usTaskLimit, taskMsg.szDestination, taskMsg.szTarget,
									bValidDatetime ? szDatetime : "null");
								LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
							}
						}
						else if (nSubType == TASK_OPT_CLOSE) { 
							int nState = -1;
							TopicTaskCloseMessage taskCloseMsg;
							bool bValidTask = false;
							bool bValidDatetime = false;
							char szDatetime[20] = { 0 };
							if (doc.HasMember("state")) {
								if (doc["state"].IsInt()) {
									nState = doc["state"].GetInt();
									taskCloseMsg.nClose = nState;
								}
							}
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
									pMsg->uiMsgSequence, bValidTask ? taskCloseMsg.szTaskId : "null", nState,
									bValidDatetime ? szDatetime : "null");
								LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
							}
						}
						else if (nSubType == TASK_OPT_MODIFY) {
							TopicTaskModifyMessage taskModifyMsg;
							bool bValidTask = false;
							bool bValidDatetime = false;
							char szDatetime[20] = { 0 };
							if (doc.HasMember("taskId")) {
								if (doc["taskId"].IsString()) {
									size_t nSize = doc["taskId"].GetStringLength();
									if (nSize) {
										strncpy_s(taskModifyMsg.szTaskId, sizeof(taskModifyMsg.szTaskId), 
											doc["taskId"].GetString(), nSize);
										bValidTask = true;
									}
								}
							}
							if (doc.HasMember("handset")) {
								if (doc["handset"].IsString()) {
									size_t nSize = doc["handset"].GetStringLength();
									if (nSize) {
										strncpy_s(taskModifyMsg.szHandset, sizeof(taskModifyMsg.szHandset),
											doc["handset"].GetString(), nSize);
									}
								}
							}
							if (doc.HasMember("datetime")) {
								if (doc["datetime"].IsString()) {
									size_t nSize = doc["datetime"].GetStringLength();
									if (nSize) {
										strncpy_s(szDatetime, sizeof(szDatetime), doc["datetime"].GetString(), nSize);
										taskModifyMsg.ulMessageTime = strdatetime2time(szDatetime);
										bValidDatetime = true;
									}
								}
							}
							if (bValidTask && bValidDatetime) {
								handleTopicTaskModifyMsg(&taskModifyMsg);
								storeTopicMsg(pMsg, taskModifyMsg.ulMessageTime);
							}
							else {
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]modify task data miss, uuid=%s, seq=%u, "
									"task=%s, handset=%s, datetime=%s\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid,
									pMsg->uiMsgSequence, bValidTask ? taskModifyMsg.szTaskId : "null", taskModifyMsg.szHandset,
									bValidDatetime ? szDatetime : "null");
								LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
							}
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]MSG_TASK data, unsupport task type=%d\r\n",
								__FUNCTION__, __LINE__, nSubType);
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_TASK msg data, uuid=%s, seq=%u, "
							"parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
					break;
				}
				case PUBMSG_BUFFER_MODIFY: {
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
									strcpy_s(szDateTime, sizeof(szDateTime), doc["datetime"].GetString());
									bValidDatetime = true;
								}
							}
						}
						if (bValidModifyObject && bValidModifyType && bValidDatetime) {
							switch (nModifyObject) {
								case BUFFER_DEVICE: {
									switch (nModifyType) {
										case BUFFER_OPERATE_NEW: {
											break;
										}
										case BUFFER_OPERATE_UPDATE: {
											break;
										}
										case BUFFER_OPERATE_DELETE: {
											if (doc.HasMember("deviceId") && doc.HasMember("factoryId") && doc.HasMember("orgId")) {
												char szDeviceId[16] = { 0 };
												char szFactoryId[4] = { 0 };
												char szOrgId[40] = { 0 };
												if (doc["deviceId"].IsString()) {
													size_t nSize = doc["deviceId"].GetStringLength();
													if (nSize) {
														strncpy_s(szDeviceId, sizeof(szDeviceId), doc["deviceId"].GetString(), nSize);
													}
												}
												if (doc["factoryId"].IsString()) {
													size_t nSize = doc["factoryId"].GetStringLength();
													if (nSize) {
														strncpy_s(szFactoryId, sizeof(szFactoryId), doc["factoryId"].GetString(), nSize);
													}
												}
												if (doc["orgId"].IsString()) {
													size_t nSize = doc["orgId"].GetStringLength();
													if (nSize) {
														strncpy_s(szOrgId, sizeof(szOrgId), doc["orgId"].GetString(), nSize);
													}
												}
												if (strlen(szDeviceId) && strlen(szFactoryId)) {
													pthread_mutex_lock(&g_mutex4DevList);
													if (zhash_size(g_deviceList)) {
														//WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
														zhash_delete(g_deviceList, szDeviceId);
													}
													pthread_mutex_unlock(&g_mutex4DevList);
												}
											}
											break;
										}
										default: {
											break;
										}
									}
									break;
								}
								case BUFFER_GUARDER: {
									switch (nModifyType) {
										case BUFFER_OPERATE_NEW: {
											break;
										}
										case BUFFER_OPERATE_UPDATE: {
											break;
										}
										case BUFFER_OPERATE_DELETE: {
											if (doc.HasMember("guarder") && doc.HasMember("orgId")) {
												char szGuarder[20] = { 0 };
												char szOrgId[40] = { 0 };
												if (doc["guarder"].IsString()) {
													size_t nSize = doc["guarder"].GetStringLength();
													if (nSize) {
														strncpy_s(szGuarder, sizeof(szGuarder), doc["guarder"].GetString(), nSize);
													}
												}
												if (doc["orgId"].IsString()) {
													size_t nSize = doc["orgId"].GetStringLength();
													if (nSize) {
														strncpy_s(szOrgId, sizeof(szOrgId), doc["orgId"].GetString(), nSize);
													}
												}
												if (strlen(szGuarder) && strlen(szOrgId)) {
													pthread_mutex_lock(&g_mutex4GuarderList);
													if (zhash_size(g_guarderList)) {
														zhash_delete(g_guarderList, szGuarder);
													}
													pthread_mutex_unlock(&g_mutex4GuarderList);
												}
											}
											break;
										}
										default: {
											break;
										}
									}
									break;
								}
								case BUFFER_ORG: {
									switch (nModifyType) {
										case BUFFER_OPERATE_NEW: {
											break;
										}
										case BUFFER_OPERATE_UPDATE: {
											break;
										}
										case BUFFER_OPERATE_DELETE: {
											if (doc.HasMember("orgId")) {
												char szOrgId[40] = { 0 };
												if (doc["orgId"].IsString()) {
													size_t nSize = doc["orgId"].GetStringLength();
													if (nSize) {
														strncpy_s(szOrgId, sizeof(szOrgId), doc["orgId"].GetString(), nSize);
													}
												}
												if (strlen(szOrgId)) {
													pthread_mutex_lock(&g_mutex4OrgList);
													if (zhash_size(g_orgList)) {
														zhash_delete(g_orgList, szOrgId);
													}
													pthread_mutex_unlock(&g_mutex4OrgList);
												}
											}
											break;
										}
										default: {
											break;
										}
									}
									break;
								}
								case BUFFER_FENCE: {
									switch (nModifyType) {
										case BUFFER_OPERATE_NEW:
										case BUFFER_OPERATE_UPDATE: {
											if (doc.HasMember("fenceId") && doc.HasMember("fenceType")
												&& doc.HasMember("fenceContent") && doc.HasMember("activeFlag")
												&& doc.HasMember("coordinate")) {
												size_t nFenceSize = sizeof(EscortFence);
												EscortFence fence;
												bool bValidFenceId = false;
												if (doc["fenceId"].IsString()) {
													size_t nSize = doc["fenceId"].GetStringLength();
													if (nSize) {
														strncpy_s(fence.szFenceId, sizeof(fence.szFenceId),
															doc["fenceId"].GetString(), nSize);
														bValidFenceId = true;
													}
												}
												if (doc["fenceType"].IsInt()) {
													fence.nFenceType = doc["fenceType"].GetInt();
												}
												if (doc["fenceContent"].IsString()) {
													size_t nSize = doc["fenceContent"].GetStringLength();
													if (nSize) {
														strncpy_s(fence.szFenceContent, sizeof(fence.szFenceContent),
															doc["fenceContent"].GetString(), nSize);
													}
												}
												if (doc["activeFlag"].IsInt()) {
													fence.nActiveFlag = (uint8_t)doc["activeFlag"].GetInt();
												}
												if (doc["coordinate"].IsInt()) {
													fence.nCoordinate = (uint8_t)doc["coordinate"].GetInt();
												}
												if (bValidFenceId && strlen(fence.szFenceContent)) {
													EscortFence * pFence = (EscortFence *)zmalloc(nFenceSize);
													memcpy_s(pFence, nFenceSize, &fence, nFenceSize);
													pthread_mutex_lock(&g_mutex4FenceList);
													zhash_update(g_fenceList, pFence->szFenceId, pFence);
													zhash_freefn(g_fenceList, pFence->szFenceId, free);
													pthread_mutex_unlock(&g_mutex4FenceList);
												}
											}
											break;
										}
										case BUFFER_OPERATE_DELETE: {
											if (doc.HasMember("fenceId")) {
												char szFenceId[16] = { 0 };
												if (doc["fenceId"].IsString()) {
													size_t nSize = doc["fenceId"].GetStringLength();
													if (nSize) {
														strncpy_s(szFenceId, sizeof(szFenceId), doc["fenceId"].GetString(), nSize);
													}
												}
												if (strlen(szFenceId)) {
													pthread_mutex_lock(&g_mutex4FenceList);
													if (zhash_size(g_fenceList)) {
														zhash_delete(g_fenceList, szFenceId);
													}
													pthread_mutex_unlock(&g_mutex4FenceList);
												}
											}
											break;
										}
										default: {
											break;
										}
									}
									break;
								}
								case BUFFER_FENCE_TASK: {
									switch (nModifyType) {
										case BUFFER_OPERATE_NEW:
										case BUFFER_OPERATE_UPDATE: {
											if (doc.HasMember("fenceTaskId") && doc.HasMember("fenceId")
												&& doc.HasMember("factoryId") && doc.HasMember("deviceId")
												&& doc.HasMember("startTime") && doc.HasMember("stopTime")
												&& doc.HasMember("state")) {
												size_t nFenceTaskSize = sizeof(EscortFenceTask);
												EscortFenceTask fenceTask;
												if (doc["fenceTaskId"].IsString()) {
													size_t nSize = doc["fenceTaskId"].GetStringLength();
													if (nSize) {
														strncpy_s(fenceTask.szFenceTaskId, sizeof(fenceTask.szFenceTaskId),
															doc["fenceTaskId"].GetString(), nSize);
													}
												}
												if (doc["fenceId"].IsString()) {
													size_t nSize = doc["fenceId"].GetStringLength();
													if (nSize) {
														strncpy_s(fenceTask.szFenceId, sizeof(fenceTask.szFenceId),
															doc["fenceId"].GetString(), nSize);
													}
												}
												if (doc["factoryId"].IsString()) {
													size_t nSize = doc["factoryId"].GetStringLength();
													if (nSize) {
														strncpy_s(fenceTask.szFactoryId, sizeof(fenceTask.szFactoryId),
															doc["factoryId"].GetString(), nSize);
													}
												}
												if (doc["deviceId"].IsString()) {
													size_t nSize = doc["deviceId"].GetStringLength();
													if (nSize) {
														strncpy_s(fenceTask.szDeviceId, sizeof(fenceTask.szDeviceId),
															doc["deviceId"].GetString(), nSize);
													}
												}
												if (doc["startTime"].IsString()) {
													size_t nSize = doc["startTime"].GetStringLength();
													if (nSize) {
														strncpy_s(fenceTask.szStartTime, sizeof(fenceTask.szStartTime),
															doc["startTime"].GetString(), nSize);
													}
												}
												if (doc["stopTime"].IsString()) {
													size_t nSize = doc["stopTime"].GetStringLength();
													if (nSize) {
														strncpy_s(fenceTask.szStopTime, sizeof(fenceTask.szStopTime),
															doc["stopTime"].GetString(), nSize);
													}
												}
												if (doc["state"].IsInt()) {
													fenceTask.nTaskState = doc["state"].GetInt();
												}
												if (strlen(fenceTask.szFenceTaskId) && strlen(fenceTask.szFenceId)
													&& strlen(fenceTask.szFactoryId) && strlen(fenceTask.szDeviceId)
													&& strlen(fenceTask.szStartTime) && strlen(fenceTask.szStopTime)) {
													if (fenceTask.nTaskState == 1) {
														pthread_mutex_lock(&g_mutex4FenceTaskList);
														zhash_delete(g_fenceTaskList, fenceTask.szFenceTaskId);
														pthread_mutex_unlock(&g_mutex4FenceTaskList);

														char szFenceTaskSql[256] = { 0 };
														sprintf_s(szFenceTaskSql, sizeof(szFenceTaskSql), "update fence_task_info set "
															"taskState=1 where fenceTaskId=%s;", fenceTask.szFenceTaskId);
														size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
														dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
															nTransactionSize);
														pTransaction->uiSqlCount = 1;
														size_t nStatementSize = sizeof(dbproxy::SqlStatement);
														pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(nStatementSize 
															* pTransaction->uiSqlCount);
														pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK_FENCE;
														size_t nLen = strlen(szFenceTaskSql);
														pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nLen;
														pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nLen + 1);
														memcpy_s(pTransaction->pSqlList[0].pStatement, nLen + 1, szFenceTaskSql, nLen);
														pTransaction->pSqlList[0].pStatement[nLen] = '\0';
														if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
															if (pTransaction) {
																if (pTransaction->pSqlList && pTransaction->uiSqlCount > 0) {
																	for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
																		if (pTransaction->pSqlList[i].uiStatementLen 
																			&& pTransaction->pSqlList[i].pStatement) {
																			free(pTransaction->pSqlList[i].pStatement);
																			pTransaction->pSqlList[i].pStatement = NULL;
																		}
																	}
																	free(pTransaction->pSqlList);
																	pTransaction->pSqlList = NULL;
																}
																free(pTransaction);
																pTransaction = NULL;
															}
														}
													}
													else {
														EscortFenceTask * pFenceTask = (EscortFenceTask *)zmalloc(nFenceTaskSize);
														memcpy_s(pFenceTask, nFenceTaskSize, &fenceTask, nFenceTaskSize);
														pthread_mutex_lock(&g_mutex4FenceTaskList);
														zhash_update(g_fenceTaskList, pFenceTask->szFenceTaskId, pFenceTask);
														zhash_freefn(g_fenceTaskList, pFenceTask->szFenceTaskId, free);
														pthread_mutex_unlock(&g_mutex4FenceTaskList);
													}
												}
											}
											break;
										}
										case BUFFER_OPERATE_DELETE: {
											if (doc.HasMember("fenceTaskId")) {
												char szFenceTaskId[16] = { 0 };
												if (doc["fenceTaskId"].IsString()) {
													size_t nSize = doc["fenceTaskId"].GetStringLength();
													if (nSize) {
														strncpy_s(szFenceTaskId, sizeof(szFenceTaskId), doc["fenceTaskId"].GetString(),
															nSize);
													}
												}
												if (strlen(szFenceTaskId)) {
													pthread_mutex_lock(&g_mutex4FenceTaskList);
													if (zhash_size(g_fenceTaskList)) {
														zhash_delete(g_fenceTaskList, szFenceTaskId);
													}
													pthread_mutex_unlock(&g_mutex4FenceTaskList);
												}
											}
											break;
										}
										default: {
											break;
										}
									}
									break;
								}
								case BUFFER_PERSON: {
									break;
								}
								case BUFFER_MESSAGE: {
									break;
								}
								default: {
									snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_BUFFER_MODIFY msg data, uuid=%s,"
										" seq=%u, object=%d, operate=%d, datetime=%s, not support object\r\n", __FUNCTION__, 
										__LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence, nModifyObject, nModifyType, szDateTime);
									LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
									break;
								}
							}
							storeTopicMsg(pMsg, strdatetime2time(szDateTime));
						}
						else {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_BUFFER_MODIFY msg data, uuid=%s"
								", seq=%u, parameter miss, object=%d, operate=%d, datetime=%s\r\n", __FUNCTION__,
								__LINE__, pMsg->szMsgUuid, pMsg->uiMsgSequence, bValidModifyObject ? nModifyObject : 0,
								bValidModifyType ? nModifyType : 0, bValidDatetime ? szDateTime : "");
							LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse MSG_BUFFER_MODIFY msg data, uuid=%s, "
							"seq=%u, parse JSON data error\r\n", __FUNCTION__, __LINE__, pMsg->szMsgUuid,
							pMsg->uiMsgSequence);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					}
					break;
				}
				case PUBMSG_ACCOUNT_LOGIN: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						char szGuarder[20] = { 0 };
						char szSession[20] = { 0 };;
						if (doc.HasMember("account")) {
							if (doc["account"].IsString()) {
								size_t nSize = doc["account"].GetStringLength();
								if (nSize) {
									strcpy_s(szGuarder, sizeof(szGuarder), doc["account"].GetString());
								}
							}
						}
						if (doc.HasMember("session")) {
							if (doc["session"].IsString()) {
								size_t nSize = doc["session"].GetStringLength();
								if (nSize) {
									strcpy_s(szSession, sizeof(szSession), doc["session"].GetString());
								}
							}
						}
						if (strlen(szGuarder) && strlen(szSession)) {
							pthread_mutex_lock(&g_mutex4GuarderList);
							if (zhash_size(g_guarderList)) {
								Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
								if (pGuarder) {
									strcpy_s(pGuarder->szCurrentSession, sizeof(pGuarder->szCurrentSession), szSession);
								}
							}
							pthread_mutex_unlock(&g_mutex4GuarderList);
						}
					}
					break;
				}
				case PUBMSG_ACCOUNT_LOGOUT: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						if (doc.HasMember("account")) {
							char szGuarder[20] = { 0 };
							if (doc["account"].IsString()) {
								size_t nSize = doc["account"].GetStringLength();
								if (nSize) {
									strcpy_s(szGuarder, sizeof(szGuarder), doc["account"].GetString());
								}
							}
							if (strlen(szGuarder)) {
								pthread_mutex_lock(&g_mutex4GuarderList);
								if (zhash_size(g_guarderList)) {
									Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
									if (pGuarder) {
										pGuarder->szCurrentSession[0] = '\0';
									}
								}
								pthread_mutex_unlock(&g_mutex4GuarderList);
							}
						}
					}
					break;
				}
				case PUBMSG_DEVICE_CHARGE: {
					rapidjson::Document doc;
					if (!doc.Parse(pMsg->szMsgBody).HasParseError()) {
						TopicDeviceChargeMessage devChargeMsg;
						memset(&devChargeMsg, 0, sizeof(TopicDeviceChargeMessage));
						if (doc.HasMember("factoryId")) {
							if (doc["factoryId"].IsString() && doc["factoryId"].GetStringLength()) {
								strcpy_s(devChargeMsg.szFactoryId, sizeof(devChargeMsg.szFactoryId), doc["factoryId"].GetString());
							}
						}
						if (doc.HasMember("deviceId")) {
							if (doc["deviceId"].IsString() && doc["deviceId"].GetStringLength()) {
								strcpy_s(devChargeMsg.szDeviceId, sizeof(devChargeMsg.szDeviceId), doc["deviceId"].GetString());
							}
						}
						if (doc.HasMember("org")) {
							if (doc["org"].IsString() && doc["org"].GetStringLength()) {
								strcpy_s(devChargeMsg.szOrg, sizeof(devChargeMsg.szOrg), doc["org"].GetString());
							}
						}
						if (doc.HasMember("state")) {
							if (doc["state"].IsInt()) {
								devChargeMsg.nState = doc["state"].GetInt();
							}
						}
						if (doc.HasMember("datetime")) {
							if (doc["datetime"].IsString() && doc["datetime"].GetStringLength()) {
								devChargeMsg.ullMsgTime = strdatetime2time(doc["datetime"].GetString());
							}
						}
						if (strlen(devChargeMsg.szDeviceId)) {
							handleTopicDeviceChargeMsg(&devChargeMsg);
						}
					}
					break;
				}
				default: {
					sprintf_s(szLog, sizeof(szLog), "[DbProxy]%s[%d]unsupport msg type: %u\r\n", __FUNCTION__, __LINE__,
						pMsg->uiMsgType);
					LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, m_usLogType);
					break;
				}
			}
		}
	} while (1);
}

void DbProxy::storeTopicMsg(TopicMessage * pMsg_, unsigned long long ulMsgTime_)
{
	if (pMsg_) {
		char szLog[512] = { 0 };
		char szSql[1024] = { 0 };
		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(ulMsgTime_, szSqlDatetime, sizeof(szSqlDatetime));
		snprintf(szSql, sizeof(szSql), "insert into message_info (msgUuid, msgType, msgSeq, msgTopic, "
			"msgBody, msgTime, msgFrom) values ('%s', %u, %u, '%s', '%s', '%s', '%s');", pMsg_->szMsgUuid, 
			pMsg_->uiMsgType, pMsg_->uiMsgSequence, pMsg_->szMsgMark, pMsg_->szMsgBody, szSqlDatetime, 
			pMsg_->szMsgFrom);
		size_t nSize = sizeof(dbproxy::SqlTransaction);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nSize);
		if (pTransaction) {
			pTransaction->uiSqlCount = 1;
			pTransaction->uiTransactionSequence = getNextInteractSequence();
			pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
			pTransaction->szTransactionFrom[0] = '\0';
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_MESSAGE;
			size_t nSqlLen = strlen(szSql);
			pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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

int DbProxy::handleTopicDeviceAliveMsg(TopicAliveMessage * pAliveMsg_)
{
	int result = -1;
	if (pAliveMsg_) {
		bool bLastest = false;
		bool bUpdateState = false;
		pthread_mutex_lock(&g_mutex4DevList);
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pAliveMsg_->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nOnline == 0) {
					pDev->deviceBasic.nOnline = 1;
					bUpdateState = true;
				}
				if (pDev->deviceBasic.ulLastActiveTime <= pAliveMsg_->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pAliveMsg_->ulMessageTime;
					pDev->deviceBasic.nBattery = pAliveMsg_->usBattery;
					if (pDev->deviceBasic.nBattery >= BATTERY_THRESHOLD) {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER) {
							pDev->deviceBasic.nStatus -= DEV_LOWPOWER;
						}
					} 
					else {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == 0) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
					bLastest = true;
				}
				result = 0;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (bLastest) {
			char szSqlDatetime[20] = { 0 };
			format_sqldatetime(pAliveMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
			char szSqlNow[20] = { 0 };
			format_sqldatetime(time(NULL), szSqlNow, sizeof(szSqlNow));
			char szDevSql[512] = { 0 };
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastCommuncation='%s', Power=%u, Online=1, "
				"lastOptTime='%s' where DeviceID='%s';", szSqlDatetime, pAliveMsg_->usBattery, szSqlNow, 
				pAliveMsg_->szDeviceId);
			dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(sizeof(dbproxy::SqlTransaction));
			pTransaction->szTransactionFrom[0] = '\0';
			pTransaction->uiSqlCount = 1;
			pTransaction->uiTransactionSequence = getNextInteractSequence();
			pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nSqlLen = strlen(szDevSql);
			pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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

int DbProxy::handleTopicDeviceOnlineMsg(TopicOnlineMessage * pOnlineMsg_)
{
	int result = -1;
	if (pOnlineMsg_) {
		bool bLastest = false;
		bool bHaveBind = false;
		bool bHaveTask = false;
		int nWorkState = 0; //0:no work, 1:guard, 2:flee
		char szGuarder[20] = { 0 };
		char szTask[20] = { 0 };
		pthread_mutex_lock(&g_mutex4DevList);
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pOnlineMsg_->szDeviceId);
			if (pDev) {
				if (pDev->deviceBasic.nOnline == 0) {
					pDev->deviceBasic.nOnline = 1;
				}
				if (pDev->deviceBasic.ulLastActiveTime < pOnlineMsg_->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pOnlineMsg_->ulMessageTime;
					pDev->deviceBasic.nBattery = pOnlineMsg_->usBattery;
					if (pDev->deviceBasic.nBattery < BATTERY_THRESHOLD) { //lowpower
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == 0) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
					else {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER) {
							pDev->deviceBasic.nStatus -= DEV_LOWPOWER;
						}
					}
					bLastest = true;
				}
				result = 0;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (bHaveBind && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (strlen(pGuarder->szTaskId)) {
					strncpy_s(szTask, sizeof(szTask), pGuarder->szTaskId, strlen(pGuarder->szTaskId));
					bHaveTask = true;
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		if (bLastest) {
			char szSqlDatetime[20] = { 0 };
			format_sqldatetime(pOnlineMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
			unsigned long long ulTime = (unsigned long long)time(NULL);
			char szSqlNow[20] = { 0 };
			format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
			char szSql[512] = { 0 };
			snprintf(szSql, sizeof(szSql), "update device_info set LastCommuncation='%s', Power=%u, Online=1, "
				"LastOptTime='%s' where DeviceID='%s';", szSqlDatetime, pOnlineMsg_->usBattery, szSqlNow, 
				pOnlineMsg_->szDeviceId);
			dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(sizeof(dbproxy::SqlTransaction));
			pTransaction->szTransactionFrom[0] = '\0';
			pTransaction->uiSqlCount = 1;
			pTransaction->uiTransactionSequence = getNextInteractSequence();
			pTransaction->ulTransactionTime = ulTime;
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nSqlLen = strlen(szSql);
			pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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

int DbProxy::handleTopicDeviceOfflineMsg(TopicOfflineMessage * pOfflineMsg_)
{
	int result = -1;
	bool bLastest = false;
	char szGuarder[20] = { 0 };
	char szTaskId[16] = { 0 };
	bool bHaveTask = true;
	if (pOfflineMsg_) {
		pthread_mutex_lock(&g_mutex4DevList);
		if (zhash_size(g_deviceList)) {
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
				pOfflineMsg_->szDeviceId);
			if (pDev) {
				result = 0;
				if (pDev->deviceBasic.nOnline) {
					pDev->deviceBasic.nOnline = 0;
				}
				if (pOfflineMsg_->ulMessageTime >= pDev->deviceBasic.ulLastActiveTime) {
					if (strlen(pDev->szBindGuard)) {
						strcpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard);
					}
					bLastest = true;
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			if (zhash_size(g_guarderList)) {
				Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
				if (pGuarder) {
					if (strlen(pGuarder->szTaskId)) {
						strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
						bHaveTask = true;
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		if (bLastest) {
			unsigned long long ulTime = (unsigned long long)time(NULL);
			char szSqlNow[20] = { 0 };
			format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
			char szDevSql[512] = { 0 };
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set Online=0, LastOptTime='%s' where DeviceID='%s';",
				szSqlNow, pOfflineMsg_->szDeviceId);
			dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
				sizeof(dbproxy::SqlTransaction));
			unsigned int uiCount = 1;
			char szAlarmSql[512] = { 0 };
			if (strlen(szTaskId) && bHaveTask) {
				snprintf(szAlarmSql, sizeof(szAlarmSql), "insert into alarm_info (TaskID, AlarmType, ActionType, "
					"RecordTime) values ('%s', 0, 0, '%s');", szTaskId, szSqlNow);
				uiCount += 1;
			}
			pTransaction->uiSqlCount = uiCount;
			pTransaction->szTransactionFrom[0] = '\0';
			pTransaction->uiTransactionSequence = getNextInteractSequence();
			pTransaction->ulTransactionTime = ulTime;
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
				* sizeof(dbproxy::SqlStatement));
			pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nSqlLen = strlen(szDevSql);
			pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
			pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
			strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szDevSql, nSqlLen);
			pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
			if (bHaveTask && strlen(szAlarmSql)) {
				size_t nAlarmSqlLen = strlen(szAlarmSql);
				pTransaction->pSqlList[1].uiStatementLen = (unsigned int)nAlarmSqlLen;
				pTransaction->pSqlList[1].pStatement = (char *)zmalloc(nAlarmSqlLen + 1);
				strncpy_s(pTransaction->pSqlList[1].pStatement, nAlarmSqlLen + 1, szAlarmSql, nAlarmSqlLen);
				pTransaction->pSqlList[1].pStatement[nAlarmSqlLen] = '\0';
			}
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
			WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pBindMsg_->szDeviceId);
			if (pDev) {
				result = 0;
				strncpy_s(pDev->szBindGuard, sizeof(pDev->szBindGuard), pBindMsg_->szGuarder, 
					strlen(pBindMsg_->szGuarder));
				pDev->ulBindTime = pBindMsg_->ulMessageTime;
				if (pDev->deviceBasic.ulLastActiveTime < pBindMsg_->ulMessageTime) {
					pDev->deviceBasic.ulLastActiveTime = pBindMsg_->ulMessageTime;
				}
				pDev->deviceBasic.nBattery = pBindMsg_->usBattery;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pBindMsg_->szGuarder);
		if (pGuarder) {
			strncpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), pBindMsg_->szDeviceId, 
				strlen(pBindMsg_->szDeviceId));
			pGuarder->usState = STATE_GUARDER_BIND;
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		
		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pBindMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		char szSqlNow[20] = { 0 };
		format_sqldatetime((unsigned long long)time(NULL), szSqlNow, sizeof(szSqlNow));
		char szDevSql[512] = { 0 };
		snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastCommuncation='%s', Power=%u, LastOptTime='%s'"
			" where DeviceID='%s';", szSqlDatetime, pBindMsg_->usBattery, szSqlNow, pBindMsg_->szDeviceId);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(sizeof(dbproxy::SqlTransaction));
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiSqlCount = 1;
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
			* sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
		size_t nSqlLen = strlen(szDevSql);
		pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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
			memset(pTask, 0, sizeof(EscortTask));
			pTask->nTaskMode = 0;
			result = 0;
			strcpy_s(pTask->szTaskId, sizeof(pTask->szTaskId), pTaskMsg_->szTaskId);
			strcpy_s(pTask->szFactoryId, sizeof(pTask->szFactoryId), pTaskMsg_->szFactoryId);
			strcpy_s(pTask->szDeviceId, sizeof(pTask->szDeviceId), pTaskMsg_->szDeviceId);
			strcpy_s(pTask->szOrg, sizeof(pTask->szOrg), pTaskMsg_->szOrg);
			strcpy_s(pTask->szGuarder, sizeof(pTask->szGuarder), pTaskMsg_->szGuarder);
			strcpy_s(pTask->szTarget, sizeof(pTask->szTarget), pTaskMsg_->szTarget);
			strcpy_s(pTask->szDestination, sizeof(pTask->szDestination), pTaskMsg_->szDestination);
			if (strlen(pTaskMsg_->szHandset)) {
				strcpy_s(pTask->szHandset, sizeof(pTask->szHandset), pTaskMsg_->szHandset);
				pTask->nTaskMode = 1;
			} 
			pTask->nTaskState = 0;
			pTask->nTaskFlee = 0;
			pTask->nTaskLimitDistance = pTaskMsg_->usTaskLimit;
			pTask->nTaskType = pTaskMsg_->usTaskType;
			format_datetime(pTaskMsg_->ulMessageTime, pTask->szTaskStartTime, sizeof(pTask->szTaskStartTime));
			pTask->szTaskStopTime[0] = '\0';
			pthread_mutex_lock(&g_mutex4TaskList);
			zhash_update(g_taskList, pTaskMsg_->szTaskId, pTask);
			zhash_freefn(g_taskList, pTaskMsg_->szTaskId, free);
			pthread_mutex_unlock(&g_mutex4TaskList);
		}

		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pTaskMsg_->szGuarder);
		if (pGuarder) {
			pGuarder->usState = STATE_GUARDER_DUTY;
			strcpy_s(pGuarder->szTaskId, sizeof(pGuarder->szTaskId), pTaskMsg_->szTaskId);
			strcpy_s(pGuarder->szBindDevice, sizeof(pGuarder->szBindDevice), pTaskMsg_->szDeviceId);
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);

		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pTaskMsg_->szDeviceId);
		if (pDev) {
			pDev->deviceBasic.ulLastActiveTime = pTaskMsg_->ulMessageTime;
			changeDeviceStatus(DEV_GUARD, pDev->deviceBasic.nStatus);
			strcpy_s(pDev->szBindGuard, sizeof(pDev->szBindGuard), pTaskMsg_->szGuarder);
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		
		char szPersonId[32] = { 0 };
		Person person;
		bool bNewPerson = false;
		if (makePerson(pTaskMsg_->szTarget, &person)) {
			strcpy_s(szPersonId, sizeof(szPersonId), person.szPersonId);
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
		format_sqldatetime((unsigned long long)time(NULL), szSqlNow, sizeof(szSqlNow));
		char szTaskSql[1024] = { 0 };
		char szDevSql[512] = { 0 };
		char szPersonSql[512] = { 0 };
		snprintf(szTaskSql, sizeof(szTaskSql), "insert into task_info (TaskID, TaskType, LimitDistance, StartTime, "
			"Destination, UserID, TaskState, PersonID, DeviceID, Handset, TaskMode) values ('%s', %u, %u, '%s', '%s', "
			"'%s', 0, '%s', '%s', '%s', %d);", pTask->szTaskId, pTask->nTaskType, pTask->nTaskLimitDistance, szSqlDatetime,
			pTask->szDestination, pTask->szGuarder, szPersonId, pTask->szDeviceId, pTask->szHandset, pTask->nTaskMode);
		sprintf_s(szDevSql, sizeof(szDevSql), "update device_info set IsUse=1, LastCommuncation='%s', LastOptTime='%s' "
			"where DeviceID='%s';", szSqlDatetime, szSqlNow, pTaskMsg_->szDeviceId);
		if (bNewPerson) {
			snprintf(szPersonSql, sizeof(szPersonSql), "insert into person_info (PersonID, PersonName, IsEscorting) values"
				" ('%s', '%s', 1);", person.szPersonId, 
				strlen(person.szPersonName) == 0 ? person.szPersonId : person.szPersonName);
		}
		else {
			snprintf(szPersonSql, sizeof(szPersonSql), "update person_info set PersonName='%s', IsEscorting=1 "
				"where PersonID='%s';", strlen(person.szPersonName) == 0 ? person.szPersonId : person.szPersonName, 
				person.szPersonId);
		}
		char szTaskExtraSql[512] = { 0 };
		if (pTask->nTaskMode == 1) {
			snprintf(szTaskExtraSql, sizeof(szTaskExtraSql), "insert into task_extra_info(TaskId, Handset, StartTime) "
				"values ('%s', '%s', '%s');", pTask->szTaskId, pTask->szHandset, szSqlDatetime);
		}
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
			sizeof(dbproxy::SqlTransaction));
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiSqlCount = (pTask->nTaskMode == 0) ? 3 : 4;
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
			* sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK;
		size_t nTaskSqlLen = strlen(szTaskSql);
		pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nTaskSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nTaskSqlLen + 1);
		strcpy_s(pTransaction->pSqlList[0].pStatement, nTaskSqlLen + 1, szTaskSql);
		pTransaction->pSqlList[0].pStatement[nTaskSqlLen] = '\0';
		pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
		size_t nDevSqlLen = strlen(szDevSql);
		pTransaction->pSqlList[1].uiStatementLen = (unsigned int)nDevSqlLen;
		pTransaction->pSqlList[1].pStatement = (char *)zmalloc(nDevSqlLen + 1);
		strcpy_s(pTransaction->pSqlList[1].pStatement, nDevSqlLen + 1, szDevSql);
		pTransaction->pSqlList[1].pStatement[nDevSqlLen] = '\0';
		pTransaction->pSqlList[2].uiCorrelativeTable = escort_db::E_TBL_PERSON;
		size_t nPersonSqlLen = strlen(szPersonSql);
		pTransaction->pSqlList[2].uiStatementLen = (unsigned int)nPersonSqlLen;
		pTransaction->pSqlList[2].pStatement = (char *)zmalloc(nPersonSqlLen + 1);
		strcpy_s(pTransaction->pSqlList[2].pStatement, nPersonSqlLen + 1, szPersonSql);
		pTransaction->pSqlList[2].pStatement[nPersonSqlLen] = '\0';
		if (strlen(szTaskExtraSql)) {
			pTransaction->pSqlList[3].uiCorrelativeTable = escort_db::E_TBL_TASK_EXTRA;
			size_t nTaskExtraSqlLen = strlen(szTaskExtraSql);
			pTransaction->pSqlList[3].uiStatementLen = (unsigned int)nTaskExtraSqlLen;
			pTransaction->pSqlList[3].pStatement = (char *)zmalloc(nTaskExtraSqlLen + 1);
			strcpy_s(pTransaction->pSqlList[3].pStatement, nTaskExtraSqlLen + 1, szTaskExtraSql);
			pTransaction->pSqlList[3].pStatement[nTaskExtraSqlLen] = '\0';
		}
		if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
			for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
				if (pTransaction->pSqlList[i].pStatement) {
					free(pTransaction->pSqlList[i].pStatement);
					pTransaction->pSqlList[i].pStatement = NULL;
					pTransaction->pSqlList[i].uiStatementLen = 0;
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
		char szHandset[64] = { 0 };
		int nTaskMode = 0;
		Person person;
		pthread_mutex_lock(&g_mutex4TaskList);
		EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, pCloseTaskMsg_->szTaskId);
		if (pTask) {
			strncpy_s(szDeviceId, sizeof(szDeviceId), pTask->szDeviceId, strlen(pTask->szDeviceId));
			strncpy_s(szGuarder, sizeof(szGuarder), pTask->szGuarder, strlen(pTask->szGuarder));
			makePerson(pTask->szTarget, &person);
			format_datetime(pCloseTaskMsg_->ulMessageTime, pTask->szTaskStopTime,
				strlen(pTask->szTaskStopTime));
			if (strlen(pTask->szHandset)) {
				nTaskMode = pTask->nTaskMode;
				strncpy_s(szHandset, sizeof(szHandset), pTask->szHandset, strlen(pTask->szHandset));
			}
		}
		zhash_delete(g_taskList, pCloseTaskMsg_->szTaskId);
		pthread_mutex_unlock(&g_mutex4TaskList);

		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
		if (pDev) {
			changeDeviceStatus(DEV_ONLINE, pDev->deviceBasic.nStatus);
			if (pDev->deviceBasic.nLooseStatus == 1) {
				pDev->deviceBasic.nStatus += DEV_LOOSE;
			}
			if (pDev->deviceBasic.nBattery < BATTERY_THRESHOLD) {
				pDev->deviceBasic.nStatus += DEV_LOWPOWER;
			}
			pDev->deviceBasic.ulLastActiveTime = pCloseTaskMsg_->ulMessageTime;
			pDev->szBindGuard[0] = '\0';
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		pthread_mutex_lock(&g_mutex4GuarderList);
		Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
		if (pGuarder) {
			pGuarder->usState = STATE_GUARDER_FREE;
			pGuarder->szTaskId[0] = '\0';
			pGuarder->szBindDevice[0] = '\0';
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);

		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pCloseTaskMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		char szSqlNow[20] = { 0 };
		format_sqldatetime((unsigned long long)time(NULL), szSqlNow, sizeof(szSqlNow));
		char szTaskSql[512] = { 0 };
		snprintf(szTaskSql, sizeof(szTaskSql), "update task_info set EndTime='%s', TaskState=%d where TaskID='%s';",
			szSqlDatetime, pCloseTaskMsg_->nClose, pCloseTaskMsg_->szTaskId);
		char szDevSql[512] = { 0 };
		snprintf(szDevSql, sizeof(szDevSql), "update device_info set IsUse=0, LastCommuncation='%s', LastOptTime='%s' "
			"where DeviceID='%s';", szSqlDatetime, szSqlNow, szDeviceId);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(sizeof(dbproxy::SqlTransaction));
		char szPersonSql[256] = { 0 };
		if (strlen(person.szPersonId)) {
			pthread_mutex_lock(&g_mutex4PersonList);
			Person * pPerson = (Person *)zhash_lookup(g_personList, person.szPersonId);
			if (pPerson) {
				pPerson->nFlee = 0;
			}
			pthread_mutex_unlock(&g_mutex4PersonList);
			snprintf(szPersonSql, sizeof(szPersonSql), "update person_info set IsEscorting=0 where PersonID='%s';", 
				person.szPersonId);
		}
		char szTaskExtraSql[512] = { 0 };
		bool bUpdateExtraTask = false;
		if (nTaskMode == 1 && strlen(szHandset)) {
			snprintf(szTaskExtraSql, sizeof(szTaskExtraSql), "update task_extra_info set StopTime ='%s' where TaskId='%s' "
				"and Handset='%s' and StopTime is null;", szSqlDatetime, pCloseTaskMsg_->szTaskId, szHandset);
			bUpdateExtraTask = true;
		}

		pTransaction->uiSqlCount = bUpdateExtraTask ? 4 : 3;
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
			* sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK;
		size_t nTaskSqlLen = strlen(szTaskSql);
		pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nTaskSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nTaskSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nTaskSqlLen + 1, szTaskSql, nTaskSqlLen);
		pTransaction->pSqlList[0].pStatement[nTaskSqlLen] = '\0';
		pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
		size_t nDevSqlLen = strlen(szDevSql);
		pTransaction->pSqlList[1].uiStatementLen = (unsigned int)nDevSqlLen;
		pTransaction->pSqlList[1].pStatement = (char *)zmalloc(nDevSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[1].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
		pTransaction->pSqlList[1].pStatement[nDevSqlLen] = '\0';
		size_t nPersonLen = strlen(szPersonSql);
		pTransaction->pSqlList[2].uiStatementLen = (unsigned int)nPersonLen;
		pTransaction->pSqlList[2].pStatement = (char *)zmalloc(nPersonLen + 1);
		strcpy_s(pTransaction->pSqlList[2].pStatement, nPersonLen + 1, szPersonSql);
		pTransaction->pSqlList[2].pStatement[nPersonLen] = '\0';
		pTransaction->pSqlList[2].uiCorrelativeTable = escort_db::E_TBL_PERSON;
		if (strlen(szTaskExtraSql)) {
			pTransaction->pSqlList[3].uiCorrelativeTable = escort_db::E_TBL_TASK_EXTRA;
			size_t nTaskExtraSqlLen = strlen(szTaskExtraSql);
			pTransaction->pSqlList[3].uiStatementLen = (unsigned int)nTaskExtraSqlLen;
			pTransaction->pSqlList[3].pStatement = (char *)zmalloc(nTaskExtraSqlLen + 1);
			strcpy_s(pTransaction->pSqlList[3].pStatement, nTaskExtraSqlLen + 1, szTaskExtraSql);
			pTransaction->pSqlList[3].pStatement[nTaskExtraSqlLen] = '\0';
		}
		if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
			for (unsigned int i = 0; i < pTransaction->uiSqlCount; i++) {
				if (pTransaction->pSqlList[i].pStatement) {
					free(pTransaction->pSqlList[i].pStatement);
					pTransaction->pSqlList[i].pStatement = NULL;
					pTransaction->pSqlList[i].uiStatementLen = 0;
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

int DbProxy::handleTopicTaskModifyMsg(TopicTaskModifyMessage * pModifyMsg_)
{
	int result = -1;
	if (pModifyMsg_) {
		if (strlen(pModifyMsg_->szTaskId)) {
			char szSqlDateTime[20] = { 0 };
			format_sqldatetime(pModifyMsg_->ulMessageTime, szSqlDateTime, 20);
			bool bChangeMode = false;
			char szPrevHandset[64] = { 0 };
			pthread_mutex_lock(&g_mutex4TaskList);
			if (zhash_size(g_taskList)) {
				EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, pModifyMsg_->szTaskId);
				if (pTask) {
					if (pTask->nTaskMode == 0) {
						pTask->nTaskMode = 1;
						bChangeMode = true;
					}
					if (strlen(pModifyMsg_->szHandset)) {
						if (strlen(pTask->szHandset)) {
							strcpy_s(szPrevHandset, sizeof(szPrevHandset), pTask->szHandset);
						}
						strncpy_s(pTask->szHandset, sizeof(pTask->szHandset), pModifyMsg_->szHandset, 
							strlen(pModifyMsg_->szHandset));
					}
					else {
						if (strlen(pTask->szHandset)) {
							strcpy_s(szPrevHandset, sizeof(szPrevHandset), pTask->szHandset);
						}
						pTask->szHandset[0] = '\0';
					}
				}
			}
			pthread_mutex_unlock(&g_mutex4TaskList);
			char szTaskSql[256] = { 0 };
			unsigned int nCount = 0;
			if (bChangeMode) {
				sprintf_s(szTaskSql, sizeof(szTaskSql), "update task_info set TaskMode = 1, Handset='%s' where TaskID='%s';",
					pModifyMsg_->szHandset, pModifyMsg_->szTaskId);
				nCount++;
			}
			else {
				sprintf_s(szTaskSql, sizeof(szTaskSql), "update task_info set Handset='%s' where TaskID='%s';",
					pModifyMsg_->szHandset, pModifyMsg_->szTaskId);
				nCount++;
			}
			char szTaskExtraSql1[256] = { 0 };
			if (strlen(pModifyMsg_->szHandset)) {
				sprintf_s(szTaskExtraSql1, sizeof(szTaskExtraSql1), "insert into task_extra_info (TaskId, Handset, StartTime) "
					"value ('%s', '%s', '%s');", pModifyMsg_->szTaskId, pModifyMsg_->szHandset, 
					szSqlDateTime);
				nCount++;
			}
			char szTaskExtraSql2[256] = { 0 };
			if (strlen(szPrevHandset)) {
				snprintf(szTaskExtraSql2, sizeof(szTaskExtraSql2), "update task_extra_info set StopTime='%s' where TaskId='%s' "
					"and Handset='%s' and StopTime is null;", szSqlDateTime, pModifyMsg_->szTaskId, szPrevHandset);
				nCount++;
			}
			size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
			int i = 0;
			dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
			pTransaction->uiSqlCount = nCount;
			pTransaction->uiTransactionSequence = getNextInteractSequence();
			pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
			pTransaction->szTransactionFrom[i] = '\0';
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(nCount * sizeof(dbproxy::SqlStatement));
			size_t nTaskSqlLen = strlen(szTaskSql);
			if (nTaskSqlLen > 0) {
				pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_TASK;
				pTransaction->pSqlList[i].uiStatementLen = (unsigned int)nTaskSqlLen;
				pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nTaskSqlLen + 1);
				memcpy_s(pTransaction->pSqlList[i].pStatement, nTaskSqlLen + 1, szTaskSql, nTaskSqlLen);
				pTransaction->pSqlList[i].pStatement[nTaskSqlLen] = '\0';
				i++;
			}
			size_t nTaskExtraSqlLen1 = strlen(szTaskExtraSql1);
			if (nTaskExtraSqlLen1) {
				pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_TASK_EXTRA;
				pTransaction->pSqlList[i].uiStatementLen = (unsigned int)nTaskExtraSqlLen1;
				pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nTaskExtraSqlLen1 + 1);
				memcpy_s(pTransaction->pSqlList[i].pStatement, nTaskExtraSqlLen1 + 1, szTaskExtraSql1, nTaskExtraSqlLen1);
				pTransaction->pSqlList[i].pStatement[nTaskExtraSqlLen1] = '\0';
				i++;
			}
			size_t nTaskExtraSqlLen2 = strlen(szTaskExtraSql2);
			if (nTaskExtraSqlLen2) {
				pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_TASK_EXTRA;
				pTransaction->pSqlList[i].uiStatementLen = (unsigned int)nTaskExtraSqlLen2;
				pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nTaskExtraSqlLen2 + 1);
				memcpy_s(pTransaction->pSqlList[i].pStatement, nTaskExtraSqlLen2 + 1, szTaskExtraSql2, nTaskExtraSqlLen2);
				pTransaction->pSqlList[i].pStatement[nTaskExtraSqlLen2] = '\0';
				i++;
			}
			if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
				for (unsigned int j = 0; j < pTransaction->uiSqlCount; j++) {
					if (pTransaction->pSqlList[j].pStatement) {
						free(pTransaction->pSqlList[j].pStatement);
						pTransaction->pSqlList[j].pStatement = NULL;
						pTransaction->pSqlList[j].uiStatementLen = 0;
					}
				}
				free(pTransaction->pSqlList);
				pTransaction->pSqlList = NULL;
				free(pTransaction);
				pTransaction = NULL;
			}
			result = 0;
		}
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
		char szTaskId[16] = { 0 };
		char szGuarder[20] = { 0 };
		int nFleeFlag = 0;
		if (pDev) {
			if (strlen(pDev->szBindGuard)) {
				strncpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard, strlen(pDev->szBindGuard));
			}
			if (pGpsLocateMsg_->nFlag == 1) { //realtime
				if (pDev->ulLastDeviceLocateTime <= pGpsLocateMsg_->ulMessageTime) {
					pDev->ulLastDeviceLocateTime = pGpsLocateMsg_->ulMessageTime;
					pDev->nLastLocateType = LOCATE_GPS;
					pDev->deviceBasic.nBattery = pGpsLocateMsg_->usBattery;
					pDev->devicePosition.dLatitude = pGpsLocateMsg_->dLat;
					pDev->devicePosition.dLngitude = pGpsLocateMsg_->dLng;
					pDev->devicePosition.usLatType = pGpsLocateMsg_->usLatType;
					pDev->devicePosition.usLngType = pGpsLocateMsg_->usLngType;
					if (pDev->deviceBasic.nBattery >= BATTERY_THRESHOLD) { //normal
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER) {
							pDev->deviceBasic.nStatus -= DEV_LOWPOWER;
						}
					}
					else { //lowpower
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == 0) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
					bLastest = true;
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		if (strlen(szTaskId)) {
			pthread_mutex_lock(&g_mutex4TaskList);
			if (zhash_size(g_taskList)) {
				EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szTaskId);
				if (pTask) {
					nFleeFlag = pTask->nTaskFlee;
				}
			}
			pthread_mutex_unlock(&g_mutex4TaskList);
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
		snprintf(szLocateSql, sizeof(szLocateSql), "insert into %s (LocationType, DeviceID, TaskID, IsOut, RecordTime, Latitude,"
			" Longitude, Speed, Course, Power, Coordinate) values (%d, '%s', '%s', %d, '%s', %.06f, %.06f, %.04f, %d, %u, %d);",
			szLocateDbName, escort_db::E_LOCATE_GPS, pGpsLocateMsg_->szDeviceId, szTaskId, nFleeFlag, szSqlDatetime, 
			pGpsLocateMsg_->dLat, pGpsLocateMsg_->dLng, pGpsLocateMsg_->dSpeed, (int)pGpsLocateMsg_->dDirection,
			pGpsLocateMsg_->usBattery, pGpsLocateMsg_->nCoordinate);
		char szLocateDbName2[32] = { 0 };
		sprintf_s(szLocateDbName2, sizeof(szLocateDbName2), "escort_locate.location_%02d", tm_locateTime.tm_mday);
		char szLocateSql2[256] = { 0 };
		sprintf_s(szLocateSql2, sizeof(szLocateSql2), "insert into %s (LocationType, DeviceID, TaskID, IsOut, RecordTime, Latitude,"
			" Longitude, Speed, Course, Power, Coordinate) values (%d, '%s', '%s', %d, '%s', %.06f, %.06f, %.04f, %d, %hu, %d);", 
			szLocateDbName2, escort_db::E_LOCATE_GPS, pGpsLocateMsg_->szDeviceId, szTaskId, nFleeFlag, szSqlDatetime, 
			pGpsLocateMsg_->dLat, pGpsLocateMsg_->dLng, pGpsLocateMsg_->dSpeed, (int)pGpsLocateMsg_->dDirection,
			pGpsLocateMsg_->usBattery, pGpsLocateMsg_->nCoordinate);
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		unsigned long long ulTime = (unsigned long long)time(NULL);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
		pTransaction->uiSqlCount = 2;
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = ulTime;
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount * sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		size_t nSqlLen = strlen(szLocateSql);
		pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szLocateSql, nSqlLen);
		pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
		pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		size_t nSqlLen2 = strlen(szLocateSql2);
		pTransaction->pSqlList[1].uiStatementLen = (unsigned int)nSqlLen2;
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
			char szDevSql[512] = { 0 };
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastLocation='%s', Latitude=%.06f, Longitude=%.06f,"
				" LocationType=%d, Power=%u, Speed=%.04f, Coruse=%d, Coordinate=%d, LastOptTime='%s', LastCommuncation='%s' "
				"where DeviceID='%s';", szSqlDatetime, pGpsLocateMsg_->dLat, pGpsLocateMsg_->dLng, escort_db::E_LOCATE_GPS, 
				pGpsLocateMsg_->usBattery, pGpsLocateMsg_->dSpeed, (int)pGpsLocateMsg_->dDirection, pGpsLocateMsg_->nCoordinate,
				szSqlNow, szSqlNow, pGpsLocateMsg_->szDeviceId);
			dbproxy::SqlTransaction * pTransaction2 = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
			pTransaction2->szTransactionFrom[0] = '\0';
			pTransaction2->uiTransactionSequence = getNextInteractSequence();
			pTransaction2->ulTransactionTime = ulTime;
			pTransaction2->uiSqlCount = 1;
			pTransaction2->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction2->uiSqlCount * sizeof(dbproxy::SqlStatement));
			pTransaction2->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nDevSqlLen = strlen(szDevSql);
			pTransaction2->pSqlList[0].uiStatementLen = (unsigned int)nDevSqlLen;
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
		char szTaskId[16] = { 0 };
		int nFleeFlag = 0;
		bool bLastest = false;
		unsigned short usBattery = pLbsLocateMsg_->usBattery;

		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pLbsLocateMsg_->szDeviceId);
		if (pDev) {
			if (strlen(pDev->szBindGuard)) {
				strcpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard);
			}
			if (pLbsLocateMsg_->usFlag == 1) {
				if (pDev->ulLastDeviceLocateTime <= pLbsLocateMsg_->ulMessageTime) {
					pDev->nLastLocateType = LOCATE_LBS;
					pDev->ulLastDeviceLocateTime = pLbsLocateMsg_->ulMessageTime;
					pDev->devicePosition.dLatitude = pLbsLocateMsg_->dLat;
					pDev->devicePosition.dLngitude = pLbsLocateMsg_->dLng;
					pDev->devicePosition.usLatType = pLbsLocateMsg_->usLatType;
					pDev->devicePosition.usLngType = pLbsLocateMsg_->usLngType;
					pDev->devicePosition.nPrecision = pLbsLocateMsg_->nPrecision;
					pDev->devicePosition.nCoordinate = pLbsLocateMsg_->nCoordinate;
					if (pDev->deviceBasic.ulLastActiveTime < pLbsLocateMsg_->ulMessageTime) {
						pDev->deviceBasic.ulLastActiveTime = pLbsLocateMsg_->ulMessageTime;
					}
					pDev->deviceBasic.nBattery = pLbsLocateMsg_->usBattery;
					if (pDev->deviceBasic.nBattery < BATTERY_THRESHOLD) {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == 0) {
							pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						}
					}
					else {
						if ((pDev->deviceBasic.nStatus & DEV_LOWPOWER) == DEV_LOWPOWER) {
							pDev->deviceBasic.nStatus -= DEV_LOWPOWER;
						}
					}
					bLastest = true;
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		if (strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}

		if (strlen(szTaskId)) {
			pthread_mutex_lock(&g_mutex4TaskList);
			if (zhash_size(g_taskList)) {
				EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szTaskId);
				if (pTask) {
					nFleeFlag = (int)pTask->nTaskFlee;
				}
			}
			pthread_mutex_unlock(&g_mutex4TaskList);
		}

		time_t nLocateTime = (time_t)pLbsLocateMsg_->ulMessageTime;
		struct tm tm_locateTime;
		localtime_s(&tm_locateTime, &nLocateTime);
		tm_locateTime.tm_year += 1900;
		tm_locateTime.tm_mon += 1;
		
		char szLocateDbName[32] = { 0 };
		snprintf(szLocateDbName, sizeof(szLocateDbName), "data%04d%02d.location_%02d", tm_locateTime.tm_year, tm_locateTime.tm_mon,
			tm_locateTime.tm_mday);
		char szSqlDatetime[20] = { 0 };
		snprintf(szSqlDatetime, sizeof(szSqlDatetime), "%04d-%02d-%02d %02d:%02d:%02d", tm_locateTime.tm_year, tm_locateTime.tm_mon,
			tm_locateTime.tm_mday, tm_locateTime.tm_hour, tm_locateTime.tm_min, tm_locateTime.tm_sec);
		char szLocateSql[256] = { 0 };
		snprintf(szLocateSql, sizeof(szLocateSql), "insert into %s (LocationType, DeviceID, TaskID, IsOut, RecordTime, Latitude, "
			"Longitude, Power, Coordinate) values (%d, '%s', '%s', %d, '%s', %.06f, %.06f, %u, %d);", szLocateDbName, 
			(int)escort_db::E_LOCATE_LBS,  pLbsLocateMsg_->szDeviceId, szTaskId, nFleeFlag, szSqlDatetime, pLbsLocateMsg_->dLat, 
			pLbsLocateMsg_->dLng, usBattery, pLbsLocateMsg_->nCoordinate);
		char szLocateSql2[256] = { 0 };
		char szLocateDbName2[32] = { 0 };
		snprintf(szLocateDbName2, sizeof(szLocateDbName2), "escort_locate.location_%02d", tm_locateTime.tm_mday);
		snprintf(szLocateSql2, sizeof(szLocateSql2), "insert into %s (LocationType, DeviceID, TaskID, IsOut, RecordTime, Latitude, "
			"Longitude, Power, Coordinate) values (%d, '%s', '%s', %d, '%s', %.06f, %.06f, %u, %d);", szLocateDbName2, 
			(int)escort_db::E_LOCATE_LBS, pLbsLocateMsg_->szDeviceId, szTaskId, nFleeFlag, szSqlDatetime, pLbsLocateMsg_->dLat, 
			pLbsLocateMsg_->dLng, usBattery, pLbsLocateMsg_->nCoordinate);
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		unsigned long long ulTime = (unsigned long long)time(NULL);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
		pTransaction->uiSqlCount = 2;
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = ulTime;
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount * sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		size_t nSqlLen = strlen(szLocateSql);
		pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szLocateSql, nSqlLen);
		pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
		pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		size_t nSqlLen2 = strlen(szLocateSql2);
		pTransaction->pSqlList[1].uiStatementLen = (unsigned int)nSqlLen2;
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
			char szDevSql[512] = { 0 };
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastLocation='%s', Latitude=%.06f, Longitude=%.06f, "
				"LocationType=%d, Power=%u, Speed=0.0000, Coruse=0, Coordinate=%d, LastOptTime='%s', LastCommuncation='%s' "
				"where DeviceID='%s';", szSqlDatetime, pLbsLocateMsg_->dLat, pLbsLocateMsg_->dLng, escort_db::E_LOCATE_LBS, usBattery,
				pLbsLocateMsg_->nCoordinate, szSqlNow, szSqlNow, pLbsLocateMsg_->szDeviceId);
			dbproxy::SqlTransaction * pTransaction2 = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
			pTransaction2->szTransactionFrom[0] = '\0';
			pTransaction2->uiTransactionSequence = getNextInteractSequence();
			pTransaction2->ulTransactionTime = ulTime;
			pTransaction2->uiSqlCount = 1;
			pTransaction2->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction2->uiSqlCount * sizeof(dbproxy::SqlStatement));
			pTransaction2->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
			size_t nDevSqlLen = strlen(szDevSql);
			pTransaction2->pSqlList[0].uiStatementLen = (unsigned int)nDevSqlLen;
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

int DbProxy::handleTopicAppLocateMsg(TopicLocateMessageApp * pLocateMsg_)
{
	int result = -1;
	if (pLocateMsg_) {
		bool bLastest = false;
		char szGuarder[20] = { 0 };
		char szTaskId[16] = { 0 };
		int nFleeFlag = 0;
		unsigned short usBattery = pLocateMsg_->usBattery;

		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, pLocateMsg_->szDeviceId);
		if (pDev) {
			if (strlen(pDev->szBindGuard)) {
				strcpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard);
			}
			if (pDev->ulLastGuarderLocateTime < pLocateMsg_->ulMessageTime) {
				pDev->ulLastGuarderLocateTime = pLocateMsg_->ulMessageTime;
				pDev->guardPosition.dLatitude = pLocateMsg_->dLat;
				pDev->guardPosition.dLngitude = pLocateMsg_->dLng;
				pDev->guardPosition.nCoordinate = pLocateMsg_->nCoordinate;
				pDev->nLastLocateType = LOCATE_APP;
				bLastest = true;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		if (strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}

		if (strlen(szTaskId)) {
			pthread_mutex_lock(&g_mutex4TaskList);
			if (zhash_size(g_taskList)) {
				EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szTaskId);
				if (pTask) {
					nFleeFlag = pTask->nTaskFlee;
				}
			}
			pthread_mutex_unlock(&g_mutex4TaskList);
		}

		time_t nLocateTime = (time_t)pLocateMsg_->ulMessageTime;
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
		snprintf(szLocateSql, sizeof(szLocateSql), "insert into %s (LocationType, DeviceID, TaskID, IsOut, RecordTime, "
			"Latitude, Longitude, Power) values (%d, '%s', '%s', %d, '%s', %.06f, %.06f, %d);", szLocateDbName, 
			escort_db::E_LOCATE_APP, pLocateMsg_->szDeviceId, szTaskId, nFleeFlag, szSqlDatetime, pLocateMsg_->dLat, 
			pLocateMsg_->dLng, usBattery);
		char szLocateDbName2[32] = { 0 };
		snprintf(szLocateDbName2, sizeof(szLocateDbName2), "escort_locate.location_%02d", tm_locateTime.tm_mday);
		char szLocateSql2[256] = { 0 };
		snprintf(szLocateSql2, sizeof(szLocateSql2), "insert into %s (LocationType, DeviceID, TaskID, IsOut, RecordTime, "
			"Latitude, Longitude, Power) values (%d, '%s', '%s', %d, '%s', %.06f, %.06f, %d);", szLocateDbName2, 
			escort_db::E_LOCATE_APP, pLocateMsg_->szDeviceId, szTaskId, nFleeFlag, szSqlDatetime, pLocateMsg_->dLat, 
			pLocateMsg_->dLng, usBattery);
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		unsigned long long ulTime = (unsigned long long)time(NULL);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
		pTransaction->uiSqlCount = 2;
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = ulTime;
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
			* sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		size_t nSqlLen = strlen(szLocateSql);
		pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szLocateSql, nSqlLen);
		pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
		size_t nSqlLen2 = strlen(szLocateSql2);
		pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_LOCATE;
		pTransaction->pSqlList[1].uiStatementLen = (unsigned int)nSqlLen2;
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

		//if (bLastest) {
		//	char szSqlNow[20] = { 0 };
		//	format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
		//	char szDevSql[512] = { 0 };
		//	snprintf(szDevSql, sizeof(szDevSql), "update device_info set LastLocation='%s', Latitude="
		//		"%.06f, Longitude=%.06f, LocationType=%d, Power=%u, Speed=0.0000, Coruse=0, LastOptTime='%s'"
		//		" where DeviceID='%s';", szSqlDatetime, pBtLocateMsg_->dLat, pBtLocateMsg_->dLng, 
		//		escort_db::E_LOCATE_APP, usBattery, szSqlNow, pBtLocateMsg_->szDeviceId);
		//	dbproxy::SqlTransaction * pTransaction2 = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
		//	pTransaction2->szTransactionFrom[0] = '\0';
		//	pTransaction2->uiTransactionSequence = getNextInteractSequence();
		//	pTransaction2->ulTransactionTime = ulTime;
		//	pTransaction2->uiSqlCount = 1;
		//	pTransaction2->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction2->uiSqlCount
		//		* sizeof(dbproxy::SqlStatement));
		//	pTransaction2->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
		//	size_t nDevSqlLen = strlen(szDevSql);
		//	pTransaction2->pSqlList[0].uiStatementLen = nDevSqlLen;
		//	pTransaction2->pSqlList[0].pStatement = (char *)zmalloc(nDevSqlLen + 1);
		//	strncpy_s(pTransaction2->pSqlList[0].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
		//	pTransaction2->pSqlList[0].pStatement[nDevSqlLen] = '\0';
		//	if (!addSqlTransaction(pTransaction2, SQLTYPE_EXECUTE)) {
		//		for (unsigned int i = 0; i < pTransaction2->uiSqlCount; ++i) {
		//			if (pTransaction2->pSqlList[i].pStatement) {
		//				free(pTransaction2->pSqlList[i].pStatement);
		//				pTransaction2->pSqlList[i].pStatement = NULL;
		//			}
		//		}
		//		free(pTransaction2->pSqlList);
		//		pTransaction2->pSqlList = NULL;
		//		free(pTransaction2);
		//		pTransaction2 = NULL;
		//	}
		//}
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
		char szTaskId[16] = { 0 };
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
				pDev->deviceBasic.nBattery = (unsigned char)pLowpoweAlarmMsg_->usBattery;
				if (pLowpoweAlarmMsg_->usMode == 0) {//lowpowe
					if (!bLowpower) {
						//changeDeviceStatus(DEV_LOWPOWER, pDev->deviceBasic.nStatus);
						pDev->deviceBasic.nStatus += DEV_LOWPOWER;
						bLastest = true;
					}
				}
				else {//normal
					if (bLowpower) { 
						pDev->deviceBasic.nStatus -= DEV_LOWPOWER;
						bLastest = true;
					}
				}
			}	
			bWork = ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD)
				|| ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE);
			if (bWork && strlen(pDev->szBindGuard)) {
				strcpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard);
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		if (bWork && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
	
		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pLowpoweAlarmMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		unsigned long long ulTime = (unsigned long long)time(NULL);
		char szSqlNow[20] = { 0 };
		format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
		char szDevSql[256] = { 0 };
		char szWarnSql[256] = { 0 };
		unsigned int uiCount = 0;
		if (bLastest) {	
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set Power=%u, LastCommuncation='%s', LastOptTime='%s' "
				"where DeviceID='%s';", pLowpoweAlarmMsg_->usBattery, szSqlDatetime, szSqlNow, pLowpoweAlarmMsg_->szDeviceId);
			uiCount += 1;
		}
		if (bWork && strlen(szTaskId)) {
			snprintf(szWarnSql, sizeof(szWarnSql), "insert into alarm_info (TaskID, AlarmType, ActionType, RecordTime) values"
				" ('%s', %d, %d, '%s');", szTaskId, escort_db::E_ALMTYPE_LOWPOWER, pLowpoweAlarmMsg_->usMode, szSqlDatetime);
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
					pTransaction->pSqlList[i].uiStatementLen = (unsigned int)nDevSqlLen;
					pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nDevSqlLen + 1);
					strcpy_s(pTransaction->pSqlList[i].pStatement, nDevSqlLen + 1, szDevSql);
					pTransaction->pSqlList[i].pStatement[nDevSqlLen] = '\0';
					i++;
				}
				if (nWarnSqlLen) {
					pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_ALARM;
					pTransaction->pSqlList[i].uiStatementLen = (unsigned int)nWarnSqlLen;
					pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nWarnSqlLen + 1);
					strcpy_s(pTransaction->pSqlList[i].pStatement, nWarnSqlLen + 1, szWarnSql);
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
		char szTaskId[16] = { 0 };
		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDev = (WristletDevice *)zhash_lookup(g_deviceList, 
			pLooseAlarmMsg_->szDeviceId);
		if (pDev) {
			if (pDev->deviceBasic.ulLastActiveTime < pLooseAlarmMsg_->ulMessageTime) {
				pDev->deviceBasic.ulLastActiveTime = pLooseAlarmMsg_->ulMessageTime;
			}
			bWork = ((pDev->deviceBasic.nStatus & DEV_GUARD) == DEV_GUARD)
				|| ((pDev->deviceBasic.nStatus & DEV_FLEE) == DEV_FLEE);
			if (bWork && strlen(pDev->szBindGuard)) {
				strcpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard);
			}
			if (pDev->ulLastLooseAlertTime < pLooseAlarmMsg_->ulMessageTime) {
				pDev->ulLastLooseAlertTime = pLooseAlarmMsg_->ulMessageTime;
				pDev->deviceBasic.nBattery = pLooseAlarmMsg_->usBattery;
				if (pLooseAlarmMsg_->usMode == 0) {//loose
					if (pDev->deviceBasic.nLooseStatus == 0) {
						pDev->deviceBasic.nStatus += DEV_LOOSE;
						pDev->deviceBasic.nLooseStatus = 1; //means true;
						bLastest = true;
					}
				}
				else {
					if (pDev->deviceBasic.nLooseStatus == 1) { //loose revoke
						pDev->deviceBasic.nStatus -= DEV_LOOSE;
						pDev->deviceBasic.nLooseStatus = 0;
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
				if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
					strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
				}
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}

		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pLooseAlarmMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		unsigned long long ulTime = (unsigned long long)time(NULL);
		char szSqlNow[20] = { 0 };
		format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
		char szDevSql[256] = { 0 };
		char szWarnSql[256] = { 0 };
		unsigned int uiCount = 0;
		if (bLastest) {
			snprintf(szDevSql, sizeof(szDevSql), "update device_info set Power=%u, LastCommuncation='%s', IsRemove=%u, "
				"LastOptTime='%s' where DeviceID='%s';", pLooseAlarmMsg_->usBattery, szSqlDatetime,
				(pLooseAlarmMsg_->usMode == 0) ? 1 : 0, szSqlNow, pLooseAlarmMsg_->szDeviceId);
			uiCount += 1;
		}
		if (bWork && strlen(szTaskId)) {
			snprintf(szWarnSql, sizeof(szWarnSql), "insert into alarm_info (TaskID, AlarmType, ActionType, RecordTime) "
				"values ('%s', %u, %u, '%s');", szTaskId, escort_db::E_ALMTYPE_LOOSE, pLooseAlarmMsg_->usMode, szSqlNow);
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
					pTransaction->pSqlList[i].uiStatementLen = (unsigned int)nDevSqlLen;
					pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nDevSqlLen + 1);
					strncpy_s(pTransaction->pSqlList[i].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
					pTransaction->pSqlList[i].pStatement[nDevSqlLen] = '\0';
					i++;
				}
				if (nWarnSqlLen) {
					pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_ALARM;
					pTransaction->pSqlList[i].uiStatementLen = (unsigned int)nWarnSqlLen;
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
		char szTaskId[16] = { 0 };
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
				pDev->deviceBasic.nBattery = (unsigned char)pFleeAlarmMsg_->usBattery;
				if (pFleeAlarmMsg_->usMode == 0) {//flee
					if (bGuard) {
						changeDeviceStatus(DEV_FLEE, pDev->deviceBasic.nStatus);
						bLastest = true;
					}
				}
				else { //guard
					if (bFlee) {
						changeDeviceStatus(DEV_GUARD, pDev->deviceBasic.nStatus);
						bLastest = true;
					}
				}
			}
			bWork = (bGuard || bFlee);
			if (bWork && strlen(pDev->szBindGuard)) {
				strcpy_s(szGuarder, sizeof(szGuarder), pDev->szBindGuard);
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);

		if (bWork && strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				if (pGuarder->usState == STATE_GUARDER_DUTY && strlen(pGuarder->szTaskId)) {
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
		unsigned long long ulTime = (unsigned long long)time(NULL);
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
			pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
			pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(uiCount
				* sizeof(dbproxy::SqlStatement));
			size_t nDevSqlLen = strlen(szDevSql);
			size_t nWarnSqlLen = strlen(szWarnSql);
			size_t nTaskSqlLen = strlen(szTaskSql);
			if (nDevSqlLen) {
				pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
				pTransaction->pSqlList[i].uiStatementLen = (unsigned int)nDevSqlLen;
				pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nDevSqlLen + 1);
				strncpy_s(pTransaction->pSqlList[i].pStatement, nDevSqlLen + 1, szDevSql, nDevSqlLen);
				pTransaction->pSqlList[i].pStatement[nDevSqlLen] = '\0';
				i++;
			}
			if (nTaskSqlLen) {
				pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_TASK;
				pTransaction->pSqlList[i].uiStatementLen = (unsigned int)nTaskSqlLen;
				pTransaction->pSqlList[i].pStatement = (char *)zmalloc(nTaskSqlLen + 1);
				strncpy_s(pTransaction->pSqlList[i].pStatement, nTaskSqlLen + 1, szTaskSql, nTaskSqlLen);
				pTransaction->pSqlList[i].pStatement[nTaskSqlLen] = '\0';
				i++;
			}
			if (nWarnSqlLen) {
				pTransaction->pSqlList[i].uiCorrelativeTable = escort_db::E_TBL_ALARM;
				pTransaction->pSqlList[i].uiStatementLen = (unsigned int)nWarnSqlLen;
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

int DbProxy::handleTopicFenceAlarmMsg(TopicAlarmMessageFence * pFenceAlarmMsg_)
{
	int result = -1;
	if (pFenceAlarmMsg_) {
		int nFenceTaskId = atoi(pFenceAlarmMsg_->szFenceTaskId);
		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pFenceAlarmMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		char szTaskId[20] = { 0 };
		char szGuarder[20] = { 0 };
		pthread_mutex_lock(&g_mutex4DevList);
		WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pFenceAlarmMsg_->szDeviceId);
		if (pDevice) {
			strncpy_s(szGuarder, sizeof(szGuarder), pDevice->szBindGuard, strlen(pDevice->szBindGuard));
			if (pFenceAlarmMsg_->nMode == 0) {
				pDevice->nDeviceFenceState = 1;
			}
			else {
				pDevice->nDeviceFenceState = 0;
			}
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		if (strlen(szGuarder)) {
			pthread_mutex_lock(&g_mutex4GuarderList);
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, szGuarder);
			if (pGuarder) {
				strncpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId, strlen(pGuarder->szTaskId));
			}
			pthread_mutex_unlock(&g_mutex4GuarderList);
		}
		char szAlarmSql[256] = { 0 };
		if (strlen(szTaskId) == 0) {
			snprintf(szAlarmSql, sizeof(szAlarmSql), "insert into alarm_info (TaskID, AlarmType, ActionType, "
				"RecordTime, latitude, lngitude, coordinate, fenceTaskId) values (null, %d, %d, '%s', %f, %f, %d, %d);",
				escort_db::E_ALMTYPE_FENCE, pFenceAlarmMsg_->nMode, szSqlDatetime, pFenceAlarmMsg_->dLatitude, 
				pFenceAlarmMsg_->dLngitude, pFenceAlarmMsg_->nCoordinate, nFenceTaskId);
		}
		else {
			snprintf(szAlarmSql, sizeof(szAlarmSql), "insert into alarm_info (TaskID, AlarmType, ActionType, "
				"RecordTime, latitude, lngitude, coordinate, fenceTaskId) values ('%s', %d, %d, '%s', %f, %f, %d, %d);",
				szTaskId, escort_db::E_ALMTYPE_FENCE, pFenceAlarmMsg_->nMode, szSqlDatetime, pFenceAlarmMsg_->dLatitude, 
				pFenceAlarmMsg_->dLngitude, pFenceAlarmMsg_->nCoordinate, nFenceTaskId);
		}
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
		pTransaction->uiSqlCount = 1;
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(sizeof(dbproxy::SqlStatement));
		size_t nAlarmSqlLen = strlen(szAlarmSql);
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_ALARM;
		pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nAlarmSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nAlarmSqlLen + 1);
		memcpy_s(pTransaction->pSqlList[0].pStatement, nAlarmSqlLen + 1, szAlarmSql, nAlarmSqlLen);
		pTransaction->pSqlList[0].pStatement[nAlarmSqlLen] = '\0';
		if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
			if (pTransaction) {
				for (int i = 0; i != pTransaction->uiSqlCount; i++) {
					if (pTransaction->pSqlList[i].pStatement) {
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
					}
				}
				if (pTransaction->pSqlList) {
					free(pTransaction->pSqlList);
					pTransaction->pSqlList = NULL;
				}
				free(pTransaction);
				pTransaction = NULL;
			}
		}
		result = 0;
	}
	return result;
}

int DbProxy::handleTopicLocateLostAlarmMsg(TopicAlarmMessageLocateLost * pLocateLostAlarmMsg_)
{
	int result = -1;
	if (pLocateLostAlarmMsg_ && strlen(pLocateLostAlarmMsg_->szGuarder) 
		&& strlen(pLocateLostAlarmMsg_->szDeviceId)) {
		char szTaskId[20] = { 0 };
		pthread_mutex_lock(&g_mutex4GuarderList);
		if (zhash_size(g_guarderList)) {
			Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pLocateLostAlarmMsg_->szGuarder);
			if (pGuarder) {
				if (strlen(pGuarder->szTaskId)) {
					strcpy_s(szTaskId, sizeof(szTaskId), pGuarder->szTaskId);
				}
			}
		}
		pthread_mutex_unlock(&g_mutex4GuarderList);
		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pLocateLostAlarmMsg_->ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		char szAlarmSql[256] = { 0 };
		if (strlen(szTaskId)) {
			sprintf_s(szAlarmSql, sizeof(szAlarmSql), "insert into alarm_info (TaskId, AlarmType, ActionType,"
				" RecordTime) value ('%s', %d, 0, '%s');", szTaskId, escort_db::E_ALMTYPE_LOST_LOCATE, szSqlDatetime);
		}
		else {
			sprintf_s(szAlarmSql, sizeof(szAlarmSql), "insert into alarm_info (TaskId, AlarmType, ActionType,"
				" RecordTime) value (null, %d, 0, '%s');", escort_db::E_ALMTYPE_LOST_LOCATE, szSqlDatetime);
		}
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
		pTransaction->uiSqlCount = 1;
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(sizeof(dbproxy::SqlStatement));
		size_t nAlarmSqlLen = strlen(szAlarmSql);
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_ALARM;
		pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nAlarmSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nAlarmSqlLen + 1);
		memcpy_s(pTransaction->pSqlList[0].pStatement, nAlarmSqlLen + 1, szAlarmSql, nAlarmSqlLen);
		pTransaction->pSqlList[0].pStatement[nAlarmSqlLen] = '\0';
		if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
			if (pTransaction) {
				for (int i = 0; i != pTransaction->uiSqlCount; i++) {
					if (pTransaction->pSqlList[i].pStatement) {
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
					}
				}
				if (pTransaction->pSqlList) {
					free(pTransaction->pSqlList);
					pTransaction->pSqlList = NULL;
				}
				free(pTransaction);
				pTransaction = NULL;
			}
		}
		result = 0;
	}
	return result;
}

int DbProxy::handleTopicPeerFenceAlarmMsg(TopicAlarmMessagePeerFence * pPeerFenceAlarmMsg_)
{
	int result = -1;
	if (pPeerFenceAlarmMsg_ && strlen(pPeerFenceAlarmMsg_->szTaskId)) {
		int nFenceTaskId = atoi(pPeerFenceAlarmMsg_->fenceAlarm.szFenceTaskId);
		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pPeerFenceAlarmMsg_->fenceAlarm.ulMessageTime, szSqlDatetime, sizeof(szSqlDatetime));
		char szAlarmSql[256] = { 0 };
		sprintf_s(szAlarmSql, sizeof(szAlarmSql), "insert into alarm_info (TaskID, AlarmType, ActionType, "
			"RecordTime, latitude, lngitude, coordinate, fenceTaskId) values ('%s', %d, 0, '%s', %f, %f, %d, %d);",
			pPeerFenceAlarmMsg_->szTaskId, escort_db::E_ALMTYPE_PEER_OVERBOUNDARY, szSqlDatetime, 
			pPeerFenceAlarmMsg_->fenceAlarm.dLatitude, pPeerFenceAlarmMsg_->fenceAlarm.dLngitude, 
			pPeerFenceAlarmMsg_->fenceAlarm.nCoordinate, nFenceTaskId);
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
		pTransaction->uiSqlCount = 1;
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(sizeof(dbproxy::SqlStatement));
		size_t nAlarmSqlLen = strlen(szAlarmSql);
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_ALARM;
		pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nAlarmSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nAlarmSqlLen + 1);
		memcpy_s(pTransaction->pSqlList[0].pStatement, nAlarmSqlLen + 1, szAlarmSql, nAlarmSqlLen);
		pTransaction->pSqlList[0].pStatement[nAlarmSqlLen] = '\0';
		if (!addSqlTransaction(pTransaction, SQLTYPE_EXECUTE)) {
			if (pTransaction) {
				for (int i = 0; i != pTransaction->uiSqlCount; i++) {
					if (pTransaction->pSqlList[i].pStatement) {
						free(pTransaction->pSqlList[i].pStatement);
						pTransaction->pSqlList[i].pStatement = NULL;
					}
				}
				if (pTransaction->pSqlList) {
					free(pTransaction->pSqlList);
					pTransaction->pSqlList = NULL;
				}
				free(pTransaction);
				pTransaction = NULL;
			}
		}
		result = 0;
	}
	return result;
}

int DbProxy::handleTopicDeviceChargeMsg(TopicDeviceChargeMessage * pDevChargeMsg_)
{
	int result = -1;
	if (pDevChargeMsg_ && strlen(pDevChargeMsg_->szDeviceId)) {
		bool bRecord = false;
		pthread_mutex_lock(&g_mutex4DevList);
		auto pDevice = (WristletDevice *)zhash_lookup(g_deviceList, pDevChargeMsg_->szDeviceId);
		if (pDevice) {
			if (pDevice->nDeviceInCharge != pDevChargeMsg_->nState) {
				bRecord = true;
			}
			pDevice->nDeviceInCharge = pDevChargeMsg_->nState;
			pDevice->deviceBasic.ulLastActiveTime = pDevChargeMsg_->ullMsgTime;
		}
		pthread_mutex_unlock(&g_mutex4DevList);
		char szSqlDatetime[20] = { 0 };
		format_sqldatetime(pDevChargeMsg_->ullMsgTime, szSqlDatetime, sizeof(szSqlDatetime));
		unsigned long long ulTime = (unsigned long long)time(NULL);
		char szSqlNow[20] = { 0 };
		format_sqldatetime(ulTime, szSqlNow, sizeof(szSqlNow));
		char szSql[512] = { 0 };
		snprintf(szSql, sizeof(szSql), "update device_info set LastCommuncation='%s', charge=%d, LastOptTime='%s' "
			"where DeviceID='%s';", szSqlDatetime, pDevChargeMsg_->nState, szSqlNow, pDevChargeMsg_->szDeviceId);
		dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(sizeof(dbproxy::SqlTransaction));
		pTransaction->szTransactionFrom[0] = '\0';
		pTransaction->uiTransactionSequence = getNextInteractSequence();
		pTransaction->ulTransactionTime = ulTime;

		pTransaction->uiSqlCount = bRecord ? 2 : 1;
		pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount * sizeof(dbproxy::SqlStatement));
		pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
		size_t nSqlLen = strlen(szSql);
		pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
		pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
		strncpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
		pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
		if (bRecord) {
			char szRecSql[256] = { 0 };
			sprintf_s(szRecSql, sizeof(szRecSql), "insert into device_charge_info (deviceId, state, recordTime) values "
				"('%s', %d, '%s')", pDevChargeMsg_->szDeviceId, pDevChargeMsg_->nState, szSqlDatetime);
			pTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_DEVICE_CHARGE;
			unsigned int uiRecSqlLen = (unsigned int)strlen(szRecSql);
			pTransaction->pSqlList[1].uiStatementLen = uiRecSqlLen;
			pTransaction->pSqlList[1].pStatement = (char *)zmalloc(uiRecSqlLen + 1);
			strcpy_s(pTransaction->pSqlList[1].pStatement, uiRecSqlLen + 1, szRecSql);
			pTransaction->pSqlList[1].pStatement[uiRecSqlLen] = '\0';
		}
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
								m_remoteLink.ulLastActiveTime = (unsigned long long)time(NULL);
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
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]interaction message unsupport type=%d\r\n",
									__FUNCTION__, __LINE__, nType);
								LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
								break;
							}
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]invalid interaction message, msg=%s\r\n",
							__FUNCTION__, __LINE__, pMsg->pMsgContents[i]);
						LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
					}
				}
				else {
					snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]parse interaction message failed, JSON "
						"data parse error: msg=%s\r\n", __FUNCTION__, __LINE__, pMsg->pMsgContents[i]);
					LOG_Log(m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
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
		replyContainer.usSqlKeyDesp = pContainer_->usSqlKeyDesp;
		replyContainer.szSqlOptKey[0] = '\0';
		size_t nContainerSize = sizeof(escort_db::SqlContainer);
		size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
		switch (pContainer_->usSqlOptTarget) {
			case escort_db::E_TBL_DEVICE: {
				if (pContainer_->usSqlOptType == escort_db::E_OPT_QUERY) {
					char szDeviceId[16] = { 0 };
					if (strlen(pContainer_->szSqlOptKey)) {
						strncpy_s(szDeviceId, sizeof(szDeviceId), pContainer_->szSqlOptKey, strlen(pContainer_->szSqlOptKey));
						if (pContainer_->usSqlKeyDesp == escort_db::E_KEY_EQUAL) {
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
								if (replyContainer.pStoreResult) {
									free(replyContainer.pStoreResult);
									replyContainer.pStoreResult = NULL;
								}
								return;
							}
							else {
								dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
									nTransactionSize);
								strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
									pIdentity_, strlen(pIdentity_));
								pTransaction->uiSqlCount = 1;
								pTransaction->uiTransactionSequence = getNextInteractSequence();
								pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
								pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
									* sizeof(dbproxy::SqlStatement));
								char szSql[512] = { 0 };
								snprintf(szSql, sizeof(szSql), "select DeviceID, FactoryID, OrgId, LastCommuncation,"
									" LastLocation, Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove "
									"from device_info where DeviceID='%s';", szDeviceId);
								size_t nSqlLen = strlen(szSql);
								pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
								pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(
								nTransactionSize);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							pTransaction->uiSqlCount = 1;
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							char szSql[512] = { 0 };
							if (pContainer_->usSqlKeyDesp == escort_db::E_KEY_NOT_EQUAL) {
								sprintf_s(szSql, sizeof(szSql), "select DeviceId, FactoryId, OrgId, LastCommuncation, "
									"LastLocation, Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove from"
									" device_info where DeviceID != '%s';", szDeviceId);
							}
							else if (pContainer_->usSqlKeyDesp == escort_db::E_KEY_LIKE_FORE) {
								sprintf_s(szSql, sizeof(szSql), "select DeviceId, FactoryId, OrgId, LastCommuncation, "
									"LastLocation, Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove from"
									" device_info where DeviceID like '%%%s';", szDeviceId);
							}
							else if (pContainer_->usSqlKeyDesp == escort_db::E_KEY_LIEK_TAIL) {
								sprintf_s(szSql, sizeof(szSql), "select DeviceId, FactoryId, OrgId, LastCommuncation, "
									"LastLocation, Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove from"
									" device_info where DeviceID like '%s%%';", szDeviceId);
							}
							else if (pContainer_->usSqlKeyDesp == escort_db::E_KEY_LIKE_FORETAIL) {
								sprintf_s(szSql, sizeof(szSql), "select DeviceId, FactoryId, OrgId, LastCommuncation, "
									"LastLocation, Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove from"
									" device_info where DeviceID like '%%%s%%';", szDeviceId);
							}
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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
							replyContainer.uiResultCount = (unsigned int)nCellCount;
							replyContainer.uiResultLen = (unsigned int)nSize;
							replyContainer.pStoreResult = (unsigned char *)zmalloc(nSize + 1);
							memcpy_s(replyContainer.pStoreResult, nSize, pDevList, nSize);
							replyContainer.pStoreResult[nSize] = '\0';
							size_t nContainerSize = sizeof(escort_db::SqlContainer);
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							size_t nFrameDataLen = nContainerSize + nSize;
							unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
							memcpy_s(pFrameData, nFrameDataLen, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, nFrameDataLen - nContainerSize + 1, pDevList, nSize);
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
							free(replyContainer.pStoreResult);
							replyContainer.pStoreResult = NULL;
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4DevList);
						if (bHitBuffer) {
							return;
						}
						else {
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
							strcpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom), pIdentity_);
							pTransaction->uiSqlCount = 1;
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select DeviceID, FactoryID, OrgId, LastCommuncation, LastLocation, Latitude, "
								"Longitude, LocationType, IsUse, Power, Online, IsRemove from device_info order by DeviceID, FactoryID;");
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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
							return;
						}
					}
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_DELETE) { 
					//only delete from buffer
					char szDeviceId[20] = { 0 };
					strncpy_s(szDeviceId, sizeof(szDeviceId), pContainer_->szSqlOptKey, strlen(pContainer_->szSqlOptKey));
					pthread_mutex_lock(&g_mutex4DevList);
					WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList, szDeviceId);
					if (pDevice) {
						escort_db::SqlContainer replyContainer;
						replyContainer.uiResultCount = 1;
						size_t nDeviceSize = sizeof(WristletDevice);
						replyContainer.pStoreResult = (unsigned char *)zmalloc(nDeviceSize + 1);
						replyContainer.uiResultLen = (unsigned int)nDeviceSize;
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
						zframe_t * frame_empty = zframe_new(NULL, 0);
						zframe_t * frame_reply = zframe_new(pFrameData, nFrameDataLen);
						zmsg_append(msg_reply, &frame_identity);
						zmsg_append(msg_reply, &frame_empty);
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
						replyContainer.usSqlKeyDesp = escort_db::E_KEY_EQUAL;
						strncpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey),
							pContainer_->szSqlOptKey, strlen(pContainer_->szSqlOptKey));
						zmsg_t * msg_reply = zmsg_new();
						zframe_t * frame_identity = zframe_from(pIdentity_);
						zframe_t * frame_empty = zframe_new(NULL, 0);
						zframe_t * frame_reply = zframe_new(&replyContainer, sizeof(escort_db::SqlContainer));
						zmsg_append(msg_reply, &frame_identity);
						zmsg_append(msg_reply, &frame_empty);
						zmsg_append(msg_reply, &frame_reply);
						zmsg_send(&msg_reply, m_reception);
					}
					zhash_delete(g_deviceList, szDeviceId);
					pthread_mutex_unlock(&g_mutex4DevList);
					return;
				}
				else if (pContainer_->usSqlOptTarget == escort_db::E_OPT_UPDATE) {
					pthread_mutex_lock(&g_mutex4DevList);
					char szDeviceId[20] = { 0 };
					strcpy_s(szDeviceId, sizeof(szDeviceId), pContainer_->szSqlOptKey);
					size_t nSize = sizeof(WristletDevice);
					WristletDevice * pDevice = (WristletDevice *)zmalloc(nSize);
					memcpy_s(pDevice, nSize, pContainer_->pStoreResult, nSize);
					zhash_update(g_deviceList, szDeviceId, pDevice);
					zhash_freefn(g_deviceList, szDeviceId, free);
					pthread_mutex_unlock(&g_mutex4DevList);
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
							memcpy_s(replyContainer.pStoreResult, replyContainer.uiResultLen, pGuarder, sizeof(Guarder));
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
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							pTransaction->uiSqlCount = 1;
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							char szSql[256] = { 0 };
							snprintf(szSql, sizeof(szSql), "select UserID, UserName, Password, OrgID, RoleType "
								"from user_info where UserID='%s';", szGuarder);
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_GUARDER;
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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
							replyContainer.uiResultCount = (unsigned int)nCellCount;
							replyContainer.uiResultLen = (unsigned int)nSize;
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
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
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
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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
							format_sqldatetime((unsigned long long)time(NULL), szSqlDatetime, sizeof(szSqlDatetime));
							char szUpdateGuarderSql[256] = { 0 };
							snprintf(szUpdateGuarderSql, sizeof(szUpdateGuarderSql), "update user_info set Password='%s', "
								"LastOptTime='%s' where UserID='%s';", pSrcGuarder->szPassword, szSqlDatetime, szGuarder);
							size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
							dbproxy::SqlTransaction * pSqlTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
							pSqlTransaction->uiSqlCount = 1;
							pSqlTransaction->uiTransactionSequence = getNextInteractSequence();
							pSqlTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							pSqlTransaction->szTransactionFrom[0] = '\0';
							pSqlTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(sizeof(dbproxy::SqlStatement));
							pSqlTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_GUARDER;
							size_t nUpdateGuarderSqlLen = strlen(szUpdateGuarderSql);
							pSqlTransaction->pSqlList[0].uiStatementLen = (unsigned int)nUpdateGuarderSqlLen;
							pSqlTransaction->pSqlList[0].pStatement = (char *)zmalloc(nUpdateGuarderSqlLen + 1);
							strncpy_s(pSqlTransaction->pSqlList[0].pStatement, nUpdateGuarderSqlLen + 1, szUpdateGuarderSql,
								nUpdateGuarderSqlLen);
							pSqlTransaction->pSqlList[0].pStatement[nUpdateGuarderSqlLen] = '\0';
							if (!addSqlTransaction(pSqlTransaction, SQLTYPE_EXECUTE)) {
								for (unsigned int i = 0; i < pSqlTransaction->uiSqlCount; i++) {
									if (pSqlTransaction->pSqlList[i].pStatement && pSqlTransaction->pSqlList[i].uiStatementLen) {
										free(pSqlTransaction->pSqlList[i].pStatement);
										pSqlTransaction->pSqlList[i].pStatement = NULL;
										pSqlTransaction->pSqlList[i].uiStatementLen = 0;
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
							replyContainer.uiResultLen = (unsigned int)nResultLen;
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
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
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
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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
							replyContainer.uiResultCount = (unsigned int)nCellCount;
							replyContainer.uiResultLen = (unsigned int)nSize;
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
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
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
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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
					strncpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey), pContainer_->szSqlOptKey,
						strlen(pContainer_->szSqlOptKey));
					replyContainer.uiSqlOptSeq = pContainer_->uiSqlOptSeq;
					replyContainer.ulSqlOptTime = pContainer_->ulSqlOptTime;
					replyContainer.usSqlOptType = escort_db::E_OPT_UPDATE;
					replyContainer.usSqlOptTarget = pContainer_->usSqlOptTarget;
					zmsg_t * msg_reply = zmsg_new();
					zframe_t * frame_identity = zframe_from(pIdentity_);
					zframe_t * frame_empty = zframe_new(NULL, 0);
					zframe_t * frame_reply = zframe_new(&replyContainer, sizeof(replyContainer));
					zmsg_append(msg_reply, &frame_identity);
					zmsg_append(msg_reply, &frame_empty);
					zmsg_append(msg_reply, &frame_reply);
					zmsg_send(&msg_reply, m_reception);
					if (strlen(pContainer_->szSqlOptKey) && pContainer_->uiResultLen && pContainer_->pStoreResult
						&& pContainer_->uiResultCount == 1) {
						char szOrgId[40] = { 0 };
						strncpy_s(szOrgId, sizeof(szOrgId), pContainer_->szSqlOptKey, strlen(pContainer_->szSqlOptKey));
						size_t nOrgSize = sizeof(Organization);
						Organization * pOrg = (Organization *)zmalloc(nOrgSize);
						memcpy_s(pOrg, nOrgSize, pContainer_->pStoreResult, pContainer_->uiResultLen);
						bool bUpdateData = false;
						pthread_mutex_lock(&g_mutex4OrgList);
						if (zhash_size(g_orgList)) {
							Organization * pDstOrg = (Organization *)zhash_lookup(g_orgList, szOrgId);
							if (pDstOrg) {
								if (strcmp(pDstOrg->szOrgName, pOrg->szOrgName) != 0) {
									strncpy_s(pDstOrg->szOrgName, sizeof(pDstOrg->szOrgName), pOrg->szOrgName, strlen(pOrg->szOrgName));
								}
								if (strcmp(pDstOrg->szParentOrgId, pOrg->szParentOrgId) != 0) {
									strncpy_s(pDstOrg->szParentOrgId, sizeof(pDstOrg->szParentOrgId), pOrg->szParentOrgId,
										strlen(pOrg->szParentOrgId));
								}
							}
						}
						pthread_mutex_unlock(&g_mutex4OrgList);
						free(pOrg);
						pOrg = NULL;
					}
				}
				break;
			}
			case escort_db::E_TBL_TASK: {
				if (pContainer_->usSqlOptType == escort_db::E_OPT_QUERY) {
					char szTaskId[16] = { 0 };
					if (strlen(pContainer_->szSqlOptKey)) {
						strncpy_s(szTaskId, sizeof(szTaskId), pContainer_->szSqlOptKey,
							strlen(pContainer_->szSqlOptKey));
						pthread_mutex_lock(&g_mutex4TaskList);
						EscortTask * pTask = (EscortTask *)zhash_lookup(g_taskList, szTaskId);
						if (pTask) {
							replyContainer.uiResultCount = 1;
							size_t nTaskLen = sizeof(EscortTask);
							size_t nResultLen = replyContainer.uiResultCount * nTaskLen;
							replyContainer.uiResultLen = (unsigned int)nResultLen;
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
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select TaskID, TaskType, LimitDistance, StartTime, "
								"Destination, UserID as GuarderID, DeviceID, task_info.PersonID, person_info.Perso"
								"nName, IsOut, Handset from task_info, person_info where TaskState = 0 and "
								"task_info.PersonID = person_info.PersonID and TaskID='%s';", szTaskId);
							size_t nSqlLen = strlen(szSql);
							pTransaction->uiSqlCount = 1;
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK;
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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
							replyContainer.uiResultCount = (unsigned int)nCellCount;
							replyContainer.uiResultLen = (unsigned int)nSize;
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
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select TaskID, TaskType, LimitDistance, StartTime, "
								"Destination, UserID as GuarderID, DeviceID, task_info.PersonID, person_info.Perso"
								"nName, IsOut, Handset from task_info, person_info where TaskState = 0 and "
								"task_info.PersonID = person_info.PersonID order by TaskID; ");
							size_t nSqlLen = strlen(szSql);
							pTransaction->uiSqlCount = 1;
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK;
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
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
							return;
						}
					}
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_DELETE) {

				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_UPDATE) {

				}
				break;
			}
			case escort_db::E_TBL_MESSAGE: {
				if (pContainer_->usSqlOptType == escort_db::E_OPT_QUERY) {
					if (strlen(pContainer_->szSqlOptKey)) {
						char szSql[512] = { 0 };
						switch (pContainer_->usSqlKeyDesp) {
							case escort_db::E_KEY_NOT_EQUAL: {
								sprintf_s(szSql, sizeof(szSql), "select msgUuid, msgType, msgSeq, msgBody, msgTime from "
									"message_info where msgUuid != '%s';", pContainer_->szSqlOptKey);
								break;
							}
							case escort_db::E_KEY_LIKE_FORE: {
								sprintf_s(szSql, sizeof(szSql), "select msgUuid, msgType, msgSeq, msgBody, msgTime from "
									"message_info where msgUuid like '%%%s';", pContainer_->szSqlOptKey);
								break;
							}
							case escort_db::E_KEY_LIEK_TAIL: {
								sprintf_s(szSql, sizeof(szSql), "select msgUuid, msgType, msgSeq, msgBody, msgTime from "
									"message_info where msgUuid like '%s%%';", pContainer_->szSqlOptKey);
								break;
							}
							case escort_db::E_KEY_LIKE_FORETAIL: {
								sprintf_s(szSql, sizeof(szSql), "select msgUuid, msgType, msgSeq, msgBody, msgTime from "
									"message_info where msgUuid like '%%%s%%';", pContainer_->szSqlOptKey);
								break;
							}
							case escort_db::E_KEY_EQUAL: {
								sprintf_s(szSql, sizeof(szSql), "select msgUuid, msgType, msgSeq, msgBody, msgTime from "
									"message_info where msgUuid='%s';", pContainer_->szSqlOptKey);
								break;
							}
						}
						if (strlen(szSql)) {
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							pTransaction->uiSqlCount = 1;
							strcpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom), pIdentity_);
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(sizeof(dbproxy::SqlStatement));
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_MESSAGE;
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								return;
							}
							else {
								if (pTransaction->pSqlList) {
									for (size_t i = 0; i < pTransaction->uiSqlCount; i++) {
										if (pTransaction->pSqlList[i].pStatement) {
											delete pTransaction->pSqlList[i].pStatement;
											pTransaction->pSqlList[i].pStatement = NULL;
										}
									}
									free(pTransaction->pSqlList);
									pTransaction->pSqlList = NULL;
								}
								free(pTransaction);
								pTransaction = NULL;
							}
						}
					}
					else {
						//not support
					}
				}
				break;
			}
			case escort_db::E_TBL_FENCE: {
				if (pContainer_->usSqlOptType == escort_db::E_OPT_QUERY) {
					char szFenceId[10];
					if (strlen(pContainer_->szSqlOptKey)) {
						strncpy_s(szFenceId, sizeof(szFenceId), pContainer_->szSqlOptKey, 
							strlen(pContainer_->szSqlOptKey));
						pthread_mutex_lock(&g_mutex4FenceList);
						EscortFence * pFence = (EscortFence *)zhash_lookup(g_fenceList, szFenceId);
						if (pFence) {
							strncpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey),
								szFenceId, strlen(szFenceId));
							replyContainer.uiResultCount = 1;
							size_t nFenceSize = sizeof(EscortFence);
							replyContainer.uiResultLen = (unsigned int)(nFenceSize * replyContainer.uiResultCount);
							replyContainer.pStoreResult = (unsigned char *)zmalloc(replyContainer.uiResultLen + 1);
							memcpy_s(replyContainer.pStoreResult, replyContainer.uiResultLen, pFence, nFenceSize);
							replyContainer.pStoreResult[replyContainer.uiResultLen] = '\0';
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4FenceList);
						if (bHitBuffer) {
							unsigned int uiFrameLen = (unsigned int)nContainerSize + replyContainer.uiResultLen;
							unsigned char * pFrameData = (unsigned char *)zmalloc(uiFrameLen + 1);
							memcpy_s(pFrameData, nContainerSize, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, replyContainer.uiResultLen + 1,
								replyContainer.pStoreResult, replyContainer.uiResultLen);
							pFrameData[uiFrameLen] = '\0';
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							zframe_t * frame_reply = zframe_new(pFrameData, uiFrameLen);
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_reply);
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
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							pTransaction->uiSqlCount = 1;
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select fenceId, fenceType, fenceContent, activeFlag, "
								"coordinate from fence_info where fenceId=%s;", szFenceId);
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_FENCE;
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (!addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								if (pTransaction->pSqlList) {
									free(pTransaction->pSqlList);
									pTransaction->pSqlList = NULL;
								}
								free(pTransaction);
								pTransaction = NULL;
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
							return;
						}
					}
					else {
						pthread_mutex_lock(&g_mutex4FenceList);
						size_t nCellCount = zhash_size(g_fenceList);
						size_t nFenceSize = sizeof(EscortFence);
						size_t nSize = nFenceSize * nCellCount;
						if (nCellCount) {
							EscortFence * pFenceList = (EscortFence *)zmalloc(nSize);
							EscortFence * pCellFence = (EscortFence *)zhash_first(g_fenceList);
							size_t i = 0;
							while (pCellFence) {
								memcpy_s(&pFenceList[i], nFenceSize, pCellFence, nFenceSize);
								pCellFence = (EscortFence *)zhash_next(g_fenceList);
								i++;
							}
							replyContainer.uiResultCount = (unsigned int)nCellCount;
							replyContainer.uiResultLen = (unsigned int)nSize;
							replyContainer.pStoreResult = (unsigned char *)zmalloc(nSize + 1);
							memcpy_s(replyContainer.pStoreResult, nSize + 1, pFenceList, nSize);
							replyContainer.pStoreResult[nSize] = '\0';
							size_t nContainerSize = sizeof(escort_db::SqlContainer);
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							unsigned int uiFrameDataLen = (unsigned int)(nContainerSize + nSize);
							unsigned char * pFrameData = (unsigned char *)zmalloc(uiFrameDataLen + 1);
							memcpy_s(pFrameData, nContainerSize, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, nSize, replyContainer.pStoreResult, nSize);
							pFrameData[uiFrameDataLen] = '\0';
							zframe_t * frame_reply = zframe_new(pFrameData, uiFrameDataLen);
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_reply);
							zmsg_send(&msg_reply, m_reception);
							bHitBuffer = true;
							free(pFrameData);
							pFrameData = NULL;
						}
						pthread_mutex_unlock(&g_mutex4FenceList);
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
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pTransaction->uiSqlCount
								* sizeof(dbproxy::SqlStatement));
							char szSql[512] = { 0 };
							snprintf(szSql, sizeof(szSql), "select fenceId, fenceType, fenceContent, activeFlag, "
								"coordinate from fence_info order by fenceId desc;");
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_FENCE;
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (!addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								if (pTransaction->pSqlList) {
									free(pTransaction->pSqlList);
									pTransaction->pSqlList = NULL;
								}
								free(pTransaction);
								pTransaction = NULL;
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
							return;
						}
					}
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_UPDATE) {
					char szFenceId[10] = { 0 };
					if (pContainer_->usSqlKeyDesp == escort_db::E_KEY_EQUAL && strlen(pContainer_->szSqlOptKey) 
						&& pContainer_->uiResultLen && pContainer_->uiResultCount == 1 && pContainer_->pStoreResult) {
						strncpy_s(szFenceId, sizeof(szFenceId), pContainer_->szSqlOptKey,
							strlen(pContainer_->szSqlOptKey));
						size_t nFenceSize = sizeof(EscortFence);
						if (pContainer_->uiResultLen >= nFenceSize) {
							EscortFence * pDstFence = (EscortFence *)zmalloc(nFenceSize);
							memcpy_s(pDstFence, nFenceSize, pContainer_->pStoreResult, nFenceSize);
							pthread_mutex_lock(&g_mutex4FenceList);
							zhash_update(g_fenceList, szFenceId, pDstFence);
							zhash_freefn(g_fenceList, szFenceId, free);
							pthread_mutex_unlock(&g_mutex4FenceList);
							bHitBuffer = true;
							replyContainer.pStoreResult = NULL;
							replyContainer.uiResultCount = 0;
							replyContainer.uiResultLen = 0;
							strncpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey), szFenceId,
								strlen(szFenceId));
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							zframe_t * frame_reply = zframe_new(&replyContainer, sizeof(replyContainer));
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_reply);
							zmsg_send(&msg_reply, m_reception);
							return;
						}
					}
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_DELETE) {
					char szFenceId[10] = { 0 };
					if (pContainer_->usSqlKeyDesp == escort_db::E_KEY_EQUAL && strlen(pContainer_->szSqlOptKey)) {
						strncpy_s(szFenceId, sizeof(szFenceId), pContainer_->szSqlOptKey, strlen(pContainer_->szSqlOptKey));
						strncpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey), szFenceId, 
							strlen(szFenceId));
						pthread_mutex_lock(&g_mutex4FenceList);
						zhash_delete(g_fenceList, szFenceId);
						pthread_mutex_unlock(&g_mutex4FenceList);
					}
				}
				break;
			}
			case escort_db::E_TBL_TASK_FENCE: {
				if (pContainer_->usSqlOptType == escort_db::E_OPT_QUERY) {
					if (strlen(pContainer_->szSqlOptKey)) { //query one
						char szFenceTaskId[16] = { 0 };
						strncpy_s(szFenceTaskId, sizeof(szFenceTaskId), pContainer_->szSqlOptKey,
							strlen(pContainer_->szSqlOptKey));
						pthread_mutex_lock(&g_mutex4FenceTaskList);
						EscortFenceTask * pFenceTask = (EscortFenceTask *)zhash_lookup(g_fenceTaskList, szFenceTaskId);
						if (pFenceTask) {
							size_t nFenceTaskSize = sizeof(EscortFenceTask);
							replyContainer.uiResultCount = 1;
							replyContainer.uiResultLen = (unsigned int)nFenceTaskSize;
							replyContainer.pStoreResult = (unsigned char *)zmalloc(replyContainer.uiResultLen + 1);
							strcpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey), szFenceTaskId);
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							zframe_t * frame_reply = zframe_new(&replyContainer, sizeof(replyContainer));
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_reply);
							zmsg_send(&msg_reply, m_reception);
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4FenceTaskList);
						if (bHitBuffer) {
							return;
						}
						else {
							size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
							char szSql[256] = { 0 };
							snprintf(szSql, sizeof(szSql), "select fenceTaskId, fenceId, factoryId, deviceId, startTime, "
								"policy, peerCheck from fence_task_info where taskState = 0 and fenceTaskId=%s;", szFenceTaskId);
							size_t nSqlLen = strlen(szSql);
							pTransaction->uiSqlCount = 1;
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(sizeof(dbproxy::SqlStatement));
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK_FENCE;
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								return;
							}
							else {
								if (pTransaction) {
									if (pTransaction->pSqlList) {
										free(pTransaction->pSqlList);
										pTransaction->pSqlList = NULL;
										pTransaction->uiSqlCount = 0;
									}
									free(pTransaction);
									pTransaction = NULL;
								}
							}
						}
					}
					else { //query all
						pthread_mutex_lock(&g_mutex4FenceTaskList);
						size_t nCellCount = zhash_size(g_fenceTaskList);
						if (nCellCount > 0) {
							size_t nCellSize = sizeof(EscortFenceTask);
							size_t nSize = nCellSize * nCellCount;
							EscortFenceTask * pFenceTaskList = (EscortFenceTask *)zmalloc(nSize);
							EscortFenceTask * pFenceTaskCell = (EscortFenceTask *)zhash_first(g_fenceTaskList);
							size_t i = 0;
							while (pFenceTaskCell) {
								memcpy_s(&pFenceTaskList[i], nCellSize, pFenceTaskCell, nCellSize);
								pFenceTaskCell = (EscortFenceTask *)zhash_next(g_fenceTaskList);
								i++;
							}
							replyContainer.uiResultCount = (unsigned int)nCellCount;
							replyContainer.uiResultLen = (unsigned int)nSize;
							replyContainer.pStoreResult = (unsigned char *)zmalloc(nSize + 1);
							memcpy_s(replyContainer.pStoreResult, nSize + 1, pFenceTaskList, nSize);
							replyContainer.pStoreResult[nSize] = '\0';
							size_t nContainerSize = sizeof(replyContainer);
							size_t nFrameDataLen = nContainerSize + nSize;
							unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
							memcpy_s(pFrameData, nFrameDataLen + 1, &replyContainer, nContainerSize);
							memcpy_s(pFrameData + nContainerSize, nSize + 1, replyContainer.pStoreResult, nSize);
							pFrameData[nFrameDataLen] = '\0';
							zframe_t *frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							zframe_t * frame_reply = zframe_new(pFrameData, nFrameDataLen);
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_reply);
							zmsg_send(&msg_reply, m_reception);
							if (pFrameData) {
								free(pFrameData);
								pFrameData = NULL;
							}
							if (replyContainer.pStoreResult) {
								free(replyContainer.pStoreResult);
								replyContainer.pStoreResult = NULL;
							}
							if (pFenceTaskList) {
								free(pFenceTaskList);
								pFenceTaskList = NULL;
							}
							bHitBuffer = true;
						}
						pthread_mutex_unlock(&g_mutex4FenceTaskList);
						if (bHitBuffer) {
							return;
						}
						else {
							size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
							char szSql[256] = { 0 };
							snprintf(szSql, sizeof(szSql), "select fenceTaskId, fenceId, factoryId, deviceId, startTime, "
								"policy, peerCheck from fence_task_info where taskState = 0 order by fenceTaskId desc;");
							size_t nSqlLen = strlen(szSql);
							pTransaction->uiSqlCount = 1;
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							strncpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom),
								pIdentity_, strlen(pIdentity_));
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(sizeof(dbproxy::SqlStatement));
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_TASK_FENCE;
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen + 1, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								return;
							}
							else {
								if (pTransaction) {
									if (pTransaction->pSqlList) {
										free(pTransaction->pSqlList);
										pTransaction->pSqlList = NULL;
										pTransaction->uiSqlCount = 0;
									}
									free(pTransaction);
									pTransaction = NULL;
								}
							}
						}
					}
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_UPDATE) {
					char szFenceTaskId[10] = { 0 };
					if (pContainer_->usSqlKeyDesp == escort_db::E_KEY_EQUAL && strlen(pContainer_->szSqlOptKey) 
						&& pContainer_->uiResultCount == 1 && pContainer_->uiResultLen && pContainer_->pStoreResult) {
						strncpy_s(szFenceTaskId, sizeof(szFenceTaskId), pContainer_->szSqlOptKey, 
							strlen(pContainer_->szSqlOptKey));
						size_t nFenceTaskSize = sizeof(EscortFenceTask);
						if (pContainer_->uiResultLen >= nFenceTaskSize) {
							EscortFenceTask * pFenceTask = (EscortFenceTask *)zmalloc(nFenceTaskSize);
							memcpy_s(pFenceTask, nFenceTaskSize, pContainer_->pStoreResult, nFenceTaskSize);
							pthread_mutex_lock(&g_mutex4FenceTaskList);
							zhash_update(g_fenceTaskList, szFenceTaskId, pFenceTask);
							zhash_freefn(g_fenceTaskList, szFenceTaskId, free);
							pthread_mutex_unlock(&g_mutex4FenceTaskList);
							replyContainer.pStoreResult = NULL;
							replyContainer.uiResultCount = 0;
							replyContainer.uiResultLen = 0;
							strncpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey), szFenceTaskId,
								strlen(szFenceTaskId));
							zframe_t * frame_identity = zframe_from(pIdentity_);
							zframe_t * frame_empty = zframe_new(NULL, 0);
							zframe_t * frame_reply = zframe_new(&replyContainer, sizeof(replyContainer));
							zmsg_t * msg_reply = zmsg_new();
							zmsg_append(msg_reply, &frame_identity);
							zmsg_append(msg_reply, &frame_empty);
							zmsg_append(msg_reply, &frame_reply);
							zmsg_send(&msg_reply, m_reception);
							return;
						}
					}
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_DELETE) {
					char szFenceTaskId[10] = { 0 };
					if (pContainer_->usSqlKeyDesp == escort_db::E_KEY_EQUAL && strlen(pContainer_->szSqlOptKey)) {
						strncpy_s(szFenceTaskId, sizeof(szFenceTaskId), pContainer_->szSqlOptKey, 
							strlen(pContainer_->szSqlOptKey));
						strncpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey), szFenceTaskId,
							strlen(szFenceTaskId));
						pthread_mutex_lock(&g_mutex4FenceTaskList);
						zhash_delete(g_fenceTaskList, szFenceTaskId);
						pthread_mutex_unlock(&g_mutex4FenceTaskList);
					}
				}
				break;
			}
			case escort_db::E_TBL_PERSON: {
				if (pContainer_->usSqlOptType == escort_db::E_OPT_QUERY) {
					if (strlen(pContainer_->szSqlOptKey)) {
						char szSql[512] = { 0 };
						switch (pContainer_->usSqlKeyDesp) {
							case escort_db::E_KEY_EQUAL: {
								bool bContinue = true;
								pthread_mutex_lock(&g_mutex4PersonList);
								Person * pPerson = (Person *)zhash_lookup(g_personList, pContainer_->szSqlOptKey);
								if (pPerson) {
									bContinue = false;
									size_t nPersonSize = sizeof(Person);
									replyContainer.uiResultCount = 1;
									replyContainer.uiResultLen = (unsigned int)nPersonSize * replyContainer.uiResultCount;
									replyContainer.pStoreResult = (unsigned char *)zmalloc(replyContainer.uiResultLen + 1);
									strcpy_s(replyContainer.szSqlOptKey, sizeof(replyContainer.szSqlOptKey), pContainer_->szSqlOptKey);
									memcpy_s(replyContainer.pStoreResult, replyContainer.uiResultLen, pPerson, nPersonSize);
									replyContainer.pStoreResult[replyContainer.uiResultLen] = '\0';
									unsigned int uiFrameDataLen = (unsigned int)nContainerSize + replyContainer.uiResultLen;
									unsigned char * pFrameData = (unsigned char *)zmalloc(uiFrameDataLen);
									memcpy_s(pFrameData, uiFrameDataLen, &replyContainer, nContainerSize);
									memcpy_s(pFrameData + nContainerSize, uiFrameDataLen - nContainerSize + 1,
										replyContainer.pStoreResult, replyContainer.uiResultLen);
									zframe_t * frame_identity = zframe_from(pIdentity_);
									zframe_t * frame_empty = zframe_new(NULL, 0);
									zframe_t * frame_body = zframe_new(pFrameData, uiFrameDataLen);
									zmsg_t * msg_reply = zmsg_new();
									zmsg_append(msg_reply, &frame_identity);
									zmsg_append(msg_reply, &frame_empty);
									zmsg_append(msg_reply, &frame_body);
									zmsg_send(&msg_reply, m_reception);
									free(pFrameData);
									pFrameData = NULL;
									if (replyContainer.pStoreResult && replyContainer.uiResultLen) {
										free(replyContainer.pStoreResult);
										replyContainer.pStoreResult = NULL;
										replyContainer.uiResultLen = 0;
										replyContainer.uiResultCount = 0;
									}
								}
								pthread_mutex_unlock(&g_mutex4PersonList);
								if (!bContinue) {
									return;
								}
								sprintf_s(szSql, sizeof(szSql), "select PersonId, PersonName, IsEscorting from person_info "
									"where PersonId = '%s';", pContainer_->szSqlOptKey);
								break;
							}
							case escort_db::E_KEY_NOT_EQUAL: {
								sprintf_s(szSql, sizeof(szSql), "select PersonId, PersonName, IsEscorting from person_info "
									"where PersonId != '%s';", pContainer_->szSqlOptKey);
								break;
							}
							case escort_db::E_KEY_LIKE_FORE: {
								sprintf_s(szSql, sizeof(szSql), "select PersonId, PersonName, IsEscorting from person_info "
									"where PersonId like '%%%s';", pContainer_->szSqlOptKey);
								break;
							}
							case escort_db::E_KEY_LIEK_TAIL: {
								sprintf_s(szSql, sizeof(szSql), "select PersonId, PersonName, IsEscorting from person_info "
									"where PersonId like '%s%%';", pContainer_->szSqlOptKey);
								break;
							}
							case escort_db::E_KEY_LIKE_FORETAIL: {
								sprintf_s(szSql, sizeof(szSql), "select PersonId, PersonName, IsEscorting from person_info "
									"where PersonId like '%%%s%%';", pContainer_->szSqlOptKey);
								break;
							}
						}
						if (strlen(szSql)) {
							dbproxy::SqlTransaction * pTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
							pTransaction->uiTransactionSequence = getNextInteractSequence();
							pTransaction->ulTransactionTime = (unsigned long long)time(NULL);
							pTransaction->uiSqlCount = 1;
							strcpy_s(pTransaction->szTransactionFrom, sizeof(pTransaction->szTransactionFrom), pIdentity_);
							pTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(sizeof(dbproxy::SqlStatement));
							pTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_PERSON;
							size_t nSqlLen = strlen(szSql);
							pTransaction->pSqlList[0].uiStatementLen = (unsigned int)nSqlLen;
							pTransaction->pSqlList[0].pStatement = (char *)zmalloc(nSqlLen + 1);
							memcpy_s(pTransaction->pSqlList[0].pStatement, nSqlLen, szSql, nSqlLen);
							pTransaction->pSqlList[0].pStatement[nSqlLen] = '\0';
							if (addSqlTransaction(pTransaction, SQLTYPE_QUERY)) {
								return;
							}
							else {
								if (pTransaction->pSqlList) {
									for (size_t i = 0; i < pTransaction->uiSqlCount; i++) {
										if (pTransaction->pSqlList[i].pStatement) {
											delete pTransaction->pSqlList[i].pStatement;
											pTransaction->pSqlList[i].pStatement = NULL;
										}
									}
									free(pTransaction->pSqlList);
									pTransaction->pSqlList = NULL;
								}
								free(pTransaction);
								pTransaction = NULL;
							}
						}
					}
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_UPDATE) {
					//support
				}
				else if (pContainer_->usSqlOptType == escort_db::E_OPT_DELETE) {
					//not support
				}
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
						sizeof(pDest_->szPersonId) - 1);
				}
				if (nBackStrSize < sizeof(pDest_->szPersonName)) {
					strncpy_s(pDest_->szPersonName, sizeof(pDest_->szPersonName), backStr.c_str(), nBackStrSize);
				}
				else {
					strncpy_s(pDest_->szPersonName, sizeof(pDest_->szPersonName), backStr.c_str(),
						sizeof(pDest_->szPersonName) - 1);
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
						sizeof(pDest_->szPersonId) - 1);
				}
				memset(pDest_->szPersonName, 0, sizeof(pDest_->szPersonName));
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

unsigned int DbProxy::getNextPipeSequence()
{
	unsigned int result = 0;
	pthread_mutex_lock(&g_mutex4PipeSequence);
	if (++g_uiPipeSequence == 0) {
		g_uiPipeSequence = 1;
	}
	result = g_uiPipeSequence;
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
	char szFenceSql[512] = { 0 };//6
	char szFenceTaskSql[512] = { 0 };//7
	sprintf_s(szPersonSql, sizeof(szPersonSql), "select PersonID, PersonName, IsEscorting from person_info "
		"order by PersonID;");
	sprintf_s(szOrgSql, sizeof(szOrgSql), "select OrgID, OrgName, ParentID from org_info order by OrgID;");
	sprintf_s(szDeviceSql, sizeof(szDeviceSql), "select DeviceID, FactoryID, OrgId, LastCommuncation, LastLocation, "
		"Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove, imei, mnc, coordinate, charge "
		"from device_info order by DeviceID, FactoryID;");
	sprintf_s(szGuarderSql, sizeof(szGuarderSql), "select UserID, UserName, Password, OrgID, RoleType, PhoneCode "
		"from user_info order by UserID;");
	sprintf_s(szTaskSql, sizeof(szTaskSql), "select TaskID, TaskType, LimitDistance, StartTime, Destination, "
		"UserID as GuarderID, DeviceID, task_info.PersonID, person_info.PersonName, IsOut, Handset from "
		"task_info, person_info where TaskState = 0 and task_info.PersonID = person_info.PersonID "
		"order by TaskID;");
	sprintf_s(szFenceSql, sizeof(szFenceSql), "select fenceId, fenceType, fenceContent, activeFlag, coordinate "
		"from fence_info order by fenceId desc;");
	sprintf_s(szFenceTaskSql, sizeof(szFenceTaskSql), "select fenceTaskId, fenceId, factoryId, deviceId, startTime, "
		"stopTime, policy, peerCheck from fence_task_info where taskState != 1 order by fenceTaskId desc;");

	size_t nTransactionSize = sizeof(dbproxy::SqlTransaction);
	dbproxy::SqlTransaction * pSqlTransaction = (dbproxy::SqlTransaction *)zmalloc(nTransactionSize);
	pSqlTransaction->uiSqlCount = 7;
	pSqlTransaction->uiTransactionSequence = getNextInteractSequence();
	pSqlTransaction->szTransactionFrom[0] = '\0';
	pSqlTransaction->ulTransactionTime = (unsigned long long)time(NULL);
	pSqlTransaction->pSqlList = (dbproxy::SqlStatement *)zmalloc(pSqlTransaction->uiSqlCount 
		* sizeof(dbproxy::SqlStatement));
	size_t nPersonSqlLen = strlen(szPersonSql);
	pSqlTransaction->pSqlList[0].uiStatementLen = (unsigned int)nPersonSqlLen;
	pSqlTransaction->pSqlList[0].uiCorrelativeTable = escort_db::E_TBL_PERSON;
	pSqlTransaction->pSqlList[0].pStatement = (char *)zmalloc(nPersonSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[0].pStatement, nPersonSqlLen + 1, szPersonSql, nPersonSqlLen);
	pSqlTransaction->pSqlList[0].pStatement[nPersonSqlLen] = '\0';
	
	size_t nOrgSqlLen = strlen(szOrgSql);
	pSqlTransaction->pSqlList[1].uiStatementLen = (unsigned int)nOrgSqlLen;
	pSqlTransaction->pSqlList[1].uiCorrelativeTable = escort_db::E_TBL_ORG;
	pSqlTransaction->pSqlList[1].pStatement = (char *)zmalloc(nOrgSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[1].pStatement, nOrgSqlLen + 1, szOrgSql, nOrgSqlLen);
	pSqlTransaction->pSqlList[1].pStatement[nOrgSqlLen] = '\0';

	size_t nDeviceSqlLen = strlen(szDeviceSql);
	pSqlTransaction->pSqlList[2].uiStatementLen = (unsigned int)nDeviceSqlLen;
	pSqlTransaction->pSqlList[2].uiCorrelativeTable = escort_db::E_TBL_DEVICE;
	pSqlTransaction->pSqlList[2].pStatement = (char *)zmalloc(nDeviceSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[2].pStatement, nDeviceSqlLen + 1, szDeviceSql, nDeviceSqlLen);
	pSqlTransaction->pSqlList[2].pStatement[nDeviceSqlLen] = '\0';

	size_t nGuarderSqlLen = strlen(szGuarderSql);
	pSqlTransaction->pSqlList[3].uiStatementLen = (unsigned int)nGuarderSqlLen;
	pSqlTransaction->pSqlList[3].uiCorrelativeTable = escort_db::E_TBL_GUARDER;
	pSqlTransaction->pSqlList[3].pStatement = (char *)zmalloc(nGuarderSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[3].pStatement, nGuarderSqlLen + 1, szGuarderSql, nGuarderSqlLen);
	pSqlTransaction->pSqlList[3].pStatement[nGuarderSqlLen] = '\0';

	size_t nTaskSqlLen = strlen(szTaskSql);
	pSqlTransaction->pSqlList[4].uiStatementLen = (unsigned int)nTaskSqlLen;
	pSqlTransaction->pSqlList[4].uiCorrelativeTable = escort_db::E_TBL_TASK;
	pSqlTransaction->pSqlList[4].pStatement = (char *)zmalloc(nTaskSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[4].pStatement, nTaskSqlLen + 1, szTaskSql, nTaskSqlLen);
	pSqlTransaction->pSqlList[4].pStatement[nTaskSqlLen] = '\0';

	size_t nFenceSqlLen = strlen(szFenceSql);
	pSqlTransaction->pSqlList[5].uiStatementLen = (unsigned int)nFenceSqlLen;
	pSqlTransaction->pSqlList[5].uiCorrelativeTable = escort_db::E_TBL_FENCE;
	pSqlTransaction->pSqlList[5].pStatement = (char *)zmalloc(nFenceSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[5].pStatement, nFenceSqlLen + 1, szFenceSql, nFenceSqlLen);
	pSqlTransaction->pSqlList[5].pStatement[nFenceSqlLen] = '\0';

	size_t nFenceTaskSqlLen = strlen(szFenceTaskSql);
	pSqlTransaction->pSqlList[6].uiStatementLen = (unsigned int)nFenceTaskSqlLen;
	pSqlTransaction->pSqlList[6].uiCorrelativeTable = escort_db::E_TBL_TASK_FENCE;
	pSqlTransaction->pSqlList[6].pStatement = (char *)zmalloc(nFenceTaskSqlLen + 1);
	strncpy_s(pSqlTransaction->pSqlList[6].pStatement, nFenceTaskSqlLen + 1, szFenceTaskSql, nFenceTaskSqlLen);
	pSqlTransaction->pSqlList[6].pStatement[nFenceTaskSqlLen] = '\0';

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
		dbproxy::UpdatePipeTask * pTask = m_updateTaskQue.front();
		m_updateTaskQue.pop();
		pthread_mutex_unlock(&m_mutex4UpdatePipe);
		if (pTask) {
			do {
				int nPipeState = getPipeState();
				if (nPipeState == dbproxy::E_PIPE_OPEN) {
					char szLastUpdateTime[20];
					format_sqldatetime(g_ulLastUpdateTime, szLastUpdateTime, sizeof(szLastUpdateTime));
					MYSQL_RES * res_ptr;
					int nErr;

					pthread_mutex_lock(&m_mutex4UpdateConn);

					char szOrgSql[256] = { 0 };
					snprintf(szOrgSql, sizeof(szOrgSql), "select OrgID, OrgName, ParentID from org_info where"
						" LastOptTime >'%s' order by OrgID;", szLastUpdateTime);
					unsigned long ulOrgSqlLen = (unsigned long)strlen(szOrgSql);
					nErr = mysql_real_query(m_updateConn, szOrgSql, ulOrgSqlLen);
					if (nErr == 0) {
						res_ptr = mysql_store_result(m_updateConn);
						if (res_ptr) {
							my_ulonglong nRowCount = mysql_num_rows(res_ptr);
							if (nRowCount > 0) {
								MYSQL_ROW row;
								size_t nCount = (size_t)nRowCount;
								size_t nOrgSqlSize = sizeof(escort_db::SqlOrg);
								escort_db::SqlOrg * pOrgList = (escort_db::SqlOrg *)zmalloc(nCount * nOrgSqlSize);
								size_t i = 0;
								while (row = mysql_fetch_row(res_ptr)) {
									strcpy_s(pOrgList[i].szOrgId, sizeof(pOrgList[i].szOrgId), row[0]);
									strcpy_s(pOrgList[i].szOrgName, sizeof(pOrgList[i].szOrgName), row[1]);
									strcpy_s(pOrgList[i].szParentOrgId, sizeof(pOrgList[i].szParentOrgId), row[2]);
									i++;
								}
								BufferUpdate bufferUpdateInfo;
								bufferUpdateInfo.usUpdateObject = BUFFER_ORG;
								bufferUpdateInfo.usUpdateDataCount = (unsigned short)nCount;
								size_t nOrgSize = sizeof(Organization);
								bufferUpdateInfo.uiUpdateDataLen = (unsigned int)(nCount + nOrgSize);
								bufferUpdateInfo.pUpdateData = (unsigned char *)zmalloc(
									bufferUpdateInfo.uiUpdateDataLen + 1);
								size_t nOffset = 0;
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
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen - nOffset,
											pOrg, nOrgSize);
										nOffset += nOrgSize;
										snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]Update from DB, execute Update OrgId=%s, OrgName=%s"
											", OrgParentId=%s\r\n", __FUNCTION__, __LINE__, pOrgList[i].szOrgId, pOrgList[i].szOrgName,
											pOrgList[i].szParentOrgId);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
										snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]Update from DB, execute insert OrgId=%s, "
											"OrgName=%s, OrgParentId=%s\r\n", __FUNCTION__, __LINE__, pOrgList[i].szOrgId,
											pOrgList[i].szOrgName, pOrgList[i].szParentOrgId);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen - nOffset,
											pOrg, nOrgSize);
										nOffset += nOrgSize;
									}
								}
								pthread_mutex_unlock(&g_mutex4OrgList);

								bufferUpdateInfo.pUpdateData[bufferUpdateInfo.uiUpdateDataLen] = '\0';
								bufferUpdateInfo.ulUpdateTime = (unsigned long)time(NULL);
								size_t nBufferUpdateSize = sizeof(BufferUpdate);
								unsigned int uiUpdatePipeSequence = getNextPipeSequence();
								MessagePayload msgPayload;
								size_t nPayloadSize = sizeof(MessagePayload);
								MAKE_PAYLOAD_MARK(msgPayload.szMsgMark);
								MAKE_PAYLOAD_VERSION(msgPayload.szMsgVersion);
								msgPayload.uiMsgType = MSG_BUFFER_MODIFY;
								msgPayload.uiMsgSequence = uiUpdatePipeSequence;
								msgPayload.uiMsgCount = 1;
								msgPayload.uiMsgDataLength = (unsigned int)nBufferUpdateSize + bufferUpdateInfo.uiUpdateDataLen;
								msgPayload.pMsgData = (unsigned char *)zmalloc(msgPayload.uiMsgDataLength + 1);
								memcpy_s(msgPayload.pMsgData, msgPayload.uiMsgDataLength, &bufferUpdateInfo,
									nBufferUpdateSize);
								memcpy_s(msgPayload.pMsgData + nBufferUpdateSize, bufferUpdateInfo.uiUpdateDataLen + 1,
									bufferUpdateInfo.pUpdateData, bufferUpdateInfo.uiUpdateDataLen);
								msgPayload.pMsgData[msgPayload.uiMsgDataLength] = '\0';
								size_t nFrameDataLen = nPayloadSize + msgPayload.uiMsgDataLength;
								unsigned char * pFrameData = (unsigned char *)zmalloc(nFrameDataLen + 1);
								memcpy_s(pFrameData, nFrameDataLen, &msgPayload, nPayloadSize);
								memcpy_s(pFrameData + nFrameDataLen, nFrameDataLen - nPayloadSize + 1,
									msgPayload.pMsgData, msgPayload.uiMsgDataLength);
								pFrameData[nFrameDataLen] = '\0';
								zmsg_t * msg_update = zmsg_new();
								zframe_t * frame_update = zframe_new(pFrameData, nFrameDataLen);
								zmsg_append(msg_update, &frame_update);
								zmsg_send(&msg_update, m_pipeline);
								free(pOrgList);
								pOrgList = NULL;
								free(pFrameData);
								pFrameData = NULL;
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
							}
							mysql_free_result(res_ptr);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute OrgList sql update Loop error=%u,"
							"%s\r\n", __FUNCTION__, __LINE__, mysql_errno(m_updateConn), mysql_error(m_updateConn));
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						if (nErr == 2013 || nErr == 2006) {
							mysql_close(m_updateConn);
							m_updateConn = mysql_init(NULL);
							if (m_updateConn && mysql_real_connect(m_updateConn, m_zkDbProxy.szDbHostIp, m_zkDbProxy.szDbUser,
								m_zkDbProxy.szDbPasswd, m_zkDbProxy.szMajorSample, m_zkDbProxy.usMasterDbPort, NULL, 0)) {
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect update db %s, ip=%s, port=%hu,user=%s"
									" at %llu\r\n", __FUNCTION__, __LINE__, m_zkDbProxy.szMajorSample, m_zkDbProxy.szDbHostIp,
									m_zkDbProxy.usMasterDbPort, m_zkDbProxy.szDbUser, (unsigned long long)time(NULL));
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								mysql_set_character_set(m_updateConn, "gb2312");
							}
						}
						break;
					}

					char szGuarderSql[256] = { 0 };
					snprintf(szGuarderSql, sizeof(szGuarderSql), "select UserID, UserName, Password, OrgID, "
						"RoleType from user_info where LastOptTime > '%s' order by UserID;", szLastUpdateTime);
					unsigned long ulGuarderSqlLen = (unsigned long)strlen(szGuarderSql);
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
									pGuarderList[i].nUserRoleType = atoi(row[4]);
									i++;
								}
								BufferUpdate bufferUpdateInfo;
								bufferUpdateInfo.usUpdateObject = BUFFER_GUARDER;
								bufferUpdateInfo.usUpdateDataCount = (unsigned short)nCount;
								size_t nGuarderSize = sizeof(Guarder);
								bufferUpdateInfo.uiUpdateDataLen = (unsigned int)(nCount * nGuarderSize);
								bufferUpdateInfo.pUpdateData = (unsigned char *)zmalloc(
									bufferUpdateInfo.uiUpdateDataLen + 1);
								size_t nOffset = 0;
								pthread_mutex_lock(&g_mutex4GuarderList);
								for (i = 0; i < nCount; i++) {
									Guarder * pGuarder = (Guarder *)zhash_lookup(g_guarderList, pGuarderList[i].szUserId);
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
										if (pGuarderList[i].nUserRoleType != (int)pGuarder->usRoleType) {
											pGuarder->usRoleType = (unsigned short)pGuarderList[i].nUserRoleType;
										}
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen
											- nOffset, pGuarder, nGuarderSize);
										nOffset += nGuarderSize;
										snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update from DB, UPDATE guarder"
											"id=%s, name=%s, password=%s, orgId=%s, roleType=%d\r\n", __FUNCTION__, __LINE__,
											pGuarderList[i].szUserId, pGuarderList[i].szUserName, pGuarderList[i].szPasswd,
											pGuarderList[i].szOrgId, pGuarderList[i].nUserRoleType);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
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
										pGuarder->usRoleType = (unsigned short)pGuarderList[i].nUserRoleType;
										pGuarder->szBindDevice[0] = '\0';
										pGuarder->szCurrentSession[0] = '\0';
										pGuarder->szLink[0] = '\0';
										pGuarder->szTaskId[0] = '\0';
										pGuarder->usState = STATE_GUARDER_FREE;
										zhash_update(g_guarderList, pGuarder->szId, pGuarder);
										zhash_freefn(g_guarderList, pGuarder->szId, free);
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen
											- nOffset, pGuarder, nGuarderSize);
										nOffset += nGuarderSize;
										snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update from DB, INSERT new guarder"
											"id=%s, name=%s, password=%s, orgId=%s, roleType=%d\r\n", __FUNCTION__, __LINE__,
											pGuarderList[i].szUserId, pGuarderList[i].szUserName, pGuarderList[i].szPasswd,
											pGuarderList[i].szOrgId, pGuarderList[i].nUserRoleType);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
									}
								}
								pthread_mutex_unlock(&g_mutex4GuarderList);

								bufferUpdateInfo.pUpdateData[bufferUpdateInfo.uiUpdateDataLen] = '\0';
								bufferUpdateInfo.ulUpdateTime = (unsigned long long)time(NULL);
								size_t nBufferUpdateSize = sizeof(BufferUpdate);
								unsigned int uiUpdatePipeSequence = getNextPipeSequence();
								MessagePayload msgPayload;
								size_t nPayloadSize = sizeof(MessagePayload);
								MAKE_PAYLOAD_MARK(msgPayload.szMsgMark);
								MAKE_PAYLOAD_VERSION(msgPayload.szMsgVersion);
								msgPayload.uiMsgType = MSG_BUFFER_MODIFY;
								msgPayload.uiMsgSequence = uiUpdatePipeSequence;
								msgPayload.uiMsgCount = 1;
								msgPayload.uiMsgDataLength = (unsigned int)nBufferUpdateSize + bufferUpdateInfo.uiUpdateDataLen;
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
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							}
							mysql_free_result(res_ptr);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update GuarderList sql at %s failed,"
							" error=%d:%s\r\n", __FUNCTION__, __LINE__, szLastUpdateTime, mysql_errno(m_updateConn),
							mysql_error(m_updateConn));
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						if (nErr == 2013 || nErr == 2006) {
							mysql_close(m_updateConn);
							m_updateConn = NULL;
							m_updateConn = mysql_init(NULL);
							if (m_updateConn && mysql_real_connect(m_updateConn, m_zkDbProxy.szDbHostIp, m_zkDbProxy.szDbUser,
								m_zkDbProxy.szDbPasswd, m_zkDbProxy.szMajorSample, m_zkDbProxy.usMasterDbPort, NULL, 0)) {
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect update db %s, ip=%s, port=%hu, user=%s "
									"at %llu\r\n", __FUNCTION__, __LINE__, m_zkDbProxy.szMajorSample, m_zkDbProxy.szDbHostIp,
									m_zkDbProxy.usMasterDbPort, m_zkDbProxy.szDbUser, (unsigned long long)time(NULL));
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								mysql_set_character_set(m_updateConn, "gb2312");
							}
						}
						break;
					}

					char szDeviceSql[256] = { 0 };
					snprintf(szDeviceSql, sizeof(szDeviceSql), "select DeviceID, FactoryId, OrgId, LastCommuncation"
						", LastLocation, Latitude, Longitude, LocationType, IsUse, Power, Online, IsRemove, imei, mnc,"
						" coordinate from device_info where LastOptTime > '%s' order by DeviceID, FactoryId;",
						szLastUpdateTime);
					unsigned long ulDeviceSqlLen = (unsigned long)strlen(szDeviceSql);
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
								memset(pDeviceList, 0, nCount * nSqlDeviceSize);
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
									if (row[12] && strlen(row[12])) { //imei
										strncpy_s(pDeviceList[i].szImei, sizeof(pDeviceList[i].szImei), row[12],
											strlen(row[12]));
									}
									if (row[13]) { //mnc
										pDeviceList[i].nMnc = atoi(row[13]);
									}
									if (row[14]) { //coordinate
										pDeviceList[i].nCoordinate = atoi(row[14]);
									}
									i++;
								}

								BufferUpdate bufferUpdateInfo;
								bufferUpdateInfo.usUpdateObject = BUFFER_DEVICE;
								bufferUpdateInfo.usUpdateDataCount = (unsigned short)nCount;
								size_t nDeviceSize = sizeof(WristletDevice);
								bufferUpdateInfo.uiUpdateDataLen = (unsigned int)(nCount * nDeviceSize);
								bufferUpdateInfo.pUpdateData = (unsigned char *)zmalloc(
									bufferUpdateInfo.uiUpdateDataLen + 1);
								size_t nOffset = 0;
								pthread_mutex_lock(&g_mutex4DevList);
								for (size_t i = 0; i < nCount; i++) {
									WristletDevice * pDevice = (WristletDevice *)zhash_lookup(g_deviceList,
										pDeviceList[i].szDeviceId);
									if (pDevice) { //check option: factoryId, OrgId, imei, mnc
										bool bUpdate = false;
										if (strcmp(pDevice->deviceBasic.szFactoryId, pDeviceList[i].szFactoryId) != 0) {
											strncpy_s(pDevice->deviceBasic.szFactoryId, sizeof(pDevice->deviceBasic.szFactoryId),
												pDeviceList[i].szFactoryId, strlen(pDeviceList[i].szFactoryId));
											bUpdate = true;
										}
										if (strcmp(pDevice->deviceBasic.szOrgId, pDeviceList[i].szOrgId) != 0) {
											strncpy_s(pDevice->deviceBasic.szOrgId, sizeof(pDevice->deviceBasic.szOrgId),
												pDeviceList[i].szOrgId, strlen(pDeviceList[i].szOrgId));
											bUpdate = true;
										}
										if (strcmp(pDevice->deviceBasic.szDeviceImei, pDeviceList[i].szImei) != 0) {
											strncpy_s(pDevice->deviceBasic.szDeviceImei, sizeof(pDevice->deviceBasic.szDeviceImei),
												pDeviceList[i].szImei, strlen(pDeviceList[i].szImei));
											bUpdate = true;
										}
										if (pDevice->deviceBasic.nDeviceMnc != pDeviceList[i].nMnc) {
											pDevice->deviceBasic.nDeviceMnc = pDeviceList[i].nMnc;
											bUpdate = true;
										}
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen - nOffset,
											pDevice, nDeviceSize);
										nOffset += nDeviceSize;
										if (bUpdate) {
											snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update from DB, UPDATE device=%s, "
												"factoryId=%s, orgId=%s, imei=%s, mnc=%d\r\n", __FUNCTION__, __LINE__,
												pDeviceList[i].szDeviceId, pDeviceList[i].szFactoryId, pDeviceList[i].szOrgId,
												pDeviceList[i].szImei, pDeviceList[i].nMnc);
											LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
										}
									}
									else {
										pDevice = (WristletDevice *)zmalloc(nDeviceSize);
										memset(pDevice, 0, nDeviceSize);
										strncpy_s(pDevice->deviceBasic.szDeviceId, sizeof(pDevice->deviceBasic.szDeviceId),
											pDeviceList[i].szDeviceId, strlen(pDeviceList[i].szDeviceId));
										strncpy_s(pDevice->deviceBasic.szFactoryId, sizeof(pDevice->deviceBasic.szFactoryId),
											pDeviceList[i].szFactoryId, strlen(pDeviceList[i].szFactoryId));
										strncpy_s(pDevice->deviceBasic.szOrgId, sizeof(pDevice->deviceBasic.szOrgId),
											pDeviceList[i].szOrgId, strlen(pDeviceList[i].szOrgId));
										pDevice->deviceBasic.nLooseStatus = pDeviceList[i].usIsRemove;
										pDevice->deviceBasic.ulLastActiveTime = 0;
										pDevice->deviceBasic.nBattery = pDeviceList[i].usBattery;
										if (pDeviceList[i].usOnline) {
											pDevice->deviceBasic.nOnline = 1;
										}
										else {
											pDevice->deviceBasic.nOnline = 0;
										}
										pDevice->deviceBasic.nStatus = DEV_ONLINE;
										if (pDevice->deviceBasic.nBattery < BATTERY_THRESHOLD) {
											pDevice->deviceBasic.nStatus += DEV_LOWPOWER;
										}
										if (pDevice->deviceBasic.nLooseStatus) {
											pDevice->deviceBasic.nStatus += DEV_LOOSE;
										}
										strcpy_s(pDevice->deviceBasic.szDeviceImei, sizeof(pDevice->deviceBasic.szDeviceImei),
											pDeviceList[i].szImei);
										pDevice->deviceBasic.nDeviceMnc = pDeviceList[i].nMnc;

										pDevice->deviceBasic.ulLastActiveTime = 0;
										pDevice->szBindGuard[0] = '\0';
										pDevice->szLinkId[0] = '\0';
										pDevice->nLastLocateType = pDeviceList[0].nLocationType;
										pDevice->ulBindTime = 0;
										pDevice->ulLastFleeAlertTime = 0;
										pDevice->ulLastDeviceLocateTime = 0;
										pDevice->ulLastGuarderLocateTime = 0;
										pDevice->ulLastLooseAlertTime = 0;
										pDevice->ulLastLowPowerAlertTime = 0;
										if (pDevice->nLastLocateType == escort_db::E_LOCATE_APP) {
											pDevice->guardPosition.dLatitude = pDeviceList[i].dLat;
											pDevice->guardPosition.dLngitude = pDeviceList[i].dLng;
											pDevice->guardPosition.usLatType = pDevice->guardPosition.usLngType = 1;
											pDevice->devicePosition.dLatitude = pDevice->devicePosition.dLngitude = 0.000000;
											pDevice->devicePosition.usLatType = pDevice->devicePosition.usLngType = 1;
											pDevice->devicePosition.nPrecision = pDevice->guardPosition.nPrecision = 0;
											pDevice->guardPosition.nCoordinate = pDeviceList[i].nCoordinate;
										}
										else {
											pDevice->devicePosition.dLatitude = pDeviceList[i].dLat;
											pDevice->devicePosition.dLngitude = pDeviceList[i].dLng;
											pDevice->devicePosition.usLatType = pDevice->devicePosition.usLngType = 1;
											pDevice->guardPosition.dLatitude = pDevice->guardPosition.dLngitude = 0.000000;
											pDevice->guardPosition.usLatType = pDevice->guardPosition.usLngType = 1;
											pDevice->devicePosition.nPrecision = pDevice->guardPosition.nPrecision = 0;
											pDevice->devicePosition.nCoordinate = pDeviceList[i].nCoordinate;
										}
										zhash_update(g_deviceList, pDevice->deviceBasic.szDeviceId, pDevice);
										zhash_freefn(g_deviceList, pDevice->deviceBasic.szDeviceId, free);
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen - nOffset,
											pDevice, nDeviceSize);
										nOffset += nDeviceSize;
										snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update from DB, INSERT device=%s, "
											"factoryId=%s, orgId=%s, battery=%u, online=%u, isRemove=%u, imei=%s, mnc=%d\r\n",
											__FUNCTION__, __LINE__, pDeviceList[i].szDeviceId, pDeviceList[i].szFactoryId,
											pDeviceList[i].szOrgId, pDeviceList[i].usBattery, pDeviceList[i].usOnline,
											pDeviceList[i].usIsRemove, pDeviceList[i].szImei, pDeviceList[i].nMnc);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
									}
								}
								pthread_mutex_unlock(&g_mutex4DevList);
								bufferUpdateInfo.ulUpdateTime = (unsigned long long)time(NULL);
								bufferUpdateInfo.pUpdateData[bufferUpdateInfo.uiUpdateDataLen] = '\0';
								size_t nBufferUpdateSize = sizeof(BufferUpdate);
								MessagePayload msgPayload;
								size_t nPayloadSize = sizeof(MessagePayload);
								MAKE_PAYLOAD_MARK(msgPayload.szMsgMark);
								MAKE_PAYLOAD_VERSION(msgPayload.szMsgVersion);
								msgPayload.uiMsgCount = 1;
								msgPayload.uiMsgType = MSG_BUFFER_MODIFY;
								msgPayload.uiMsgSequence = getNextPipeSequence();
								msgPayload.uiMsgDataLength = (unsigned int)nBufferUpdateSize + bufferUpdateInfo.uiUpdateDataLen;
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
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							}
							mysql_free_result(res_ptr);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update DeviceList Sql at %s failed,"
							" error=%d,%s\r\n", __FUNCTION__, __LINE__, szLastUpdateTime, mysql_errno(m_updateConn),
							mysql_error(m_updateConn));
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						if (nErr == 2013 || nErr == 2006) {
							mysql_close(m_updateConn);
							m_updateConn = NULL;
							m_updateConn = mysql_init(NULL);
							if (m_updateConn && mysql_real_connect(m_updateConn, m_zkDbProxy.szDbHostIp, m_zkDbProxy.szDbUser,
								m_zkDbProxy.szDbPasswd, m_zkDbProxy.szMajorSample, m_zkDbProxy.usMasterDbPort, NULL, 0)) {
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect update db %s, ip=%s, port=%hu, user=%s "
									"at %llu\r\n", __FUNCTION__, __LINE__, m_zkDbProxy.szMajorSample, m_zkDbProxy.szDbHostIp,
									m_zkDbProxy.usMasterDbPort, m_zkDbProxy.szDbUser, (unsigned long long)time(NULL));
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								mysql_set_character_set(m_updateConn, "gb2312");
							}
						}
					}

					char szFenceSql[256] = { 0 };
					snprintf(szFenceSql, sizeof(szFenceSql), "select fenceId, fenceType, fenceContent, activeFlag,"
						"coordinate from fence_info where lastOptTime > '%s' order by fenceId desc;", szLastUpdateTime);
					unsigned long ulFenceSqlLen = (unsigned long)strlen(szFenceSql);
					nErr = mysql_real_query(m_updateConn, szFenceSql, ulFenceSqlLen);
					if (nErr == 0) {
						res_ptr = mysql_store_result(m_updateConn);
						if (res_ptr) {
							my_ulonglong nRowCount = mysql_num_rows(res_ptr);
							if (nRowCount > 0) {
								size_t nCount = (size_t)nRowCount;
								size_t nSqlGuarderSize = sizeof(escort_db::SqlFence);
								escort_db::SqlFence * pSqlFenceList = (escort_db::SqlFence *)zmalloc(nCount * nSqlGuarderSize);
								size_t i = 0;
								MYSQL_ROW row;
								while (row = mysql_fetch_row(res_ptr)) {
									pSqlFenceList[i].nFenceId = atoi(row[0]);
									pSqlFenceList[i].usFenceType = (unsigned short)atoi(row[1]);
									strncpy_s(pSqlFenceList[i].szFenceContent, sizeof(pSqlFenceList[i].szFenceContent),
										row[2], strlen(row[2]));
									pSqlFenceList[i].nFenceActive = (uint8_t)atoi(row[3]);
									pSqlFenceList[i].nCoordinate = (uint8_t)atoi(row[4]);
									i++;
								}

								size_t nOffset = 0;
								size_t nFenceSize = sizeof(EscortFence);
								BufferUpdate bufferUpdateInfo;
								bufferUpdateInfo.usUpdateObject = BUFFER_FENCE;
								bufferUpdateInfo.usUpdateDataCount = (unsigned short)nCount;
								bufferUpdateInfo.uiUpdateDataLen = (unsigned int)(nCount * nFenceSize);
								bufferUpdateInfo.pUpdateData = (unsigned char *)zmalloc(bufferUpdateInfo.uiUpdateDataLen + 1);
								pthread_mutex_lock(&g_mutex4FenceList);
								for (i = 0; i < nCount; i++) {
									char szFenceId[10] = { 0 };
									sprintf_s(szFenceId, sizeof(szFenceId), "%d", pSqlFenceList[i].nFenceId);
									EscortFence * pFence = (EscortFence *)zhash_lookup(g_fenceList, szFenceId);
									if (pFence) {
										if (strcmp(pFence->szFenceContent, pSqlFenceList[i].szFenceContent) != 0) {
											strncpy_s(pFence->szFenceContent, sizeof(pFence->szFenceContent),
												pSqlFenceList[i].szFenceContent, strlen(pSqlFenceList[i].szFenceContent));
										}
										if (pFence->nActiveFlag != pSqlFenceList[i].nFenceActive) {
											pFence->nActiveFlag = pSqlFenceList[i].nFenceActive;
										}
										if (pFence->nCoordinate != pSqlFenceList[i].nCoordinate) {
											pFence->nCoordinate = pSqlFenceList[i].nCoordinate;
										}
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset,
											bufferUpdateInfo.uiUpdateDataLen - nOffset, pFence, nFenceSize);
										nOffset += nFenceSize;
										snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update from DB, UPDATE fence "
											"id=%d, type=%d, content=%s, active=%d, coordinate=%d\r\n", __FUNCTION__, __LINE__,
											pSqlFenceList[i].nFenceId, pSqlFenceList[i].usFenceType,
											pSqlFenceList[i].szFenceContent, pSqlFenceList[i].nFenceActive,
											pSqlFenceList[i].nCoordinate);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
									}
									else {
										EscortFence * pFence = (EscortFence *)zmalloc(nFenceSize);
										memset(pFence, 0, nFenceSize);
										sprintf_s(pFence->szFenceId, sizeof(pFence->szFenceId), "%d", pSqlFenceList[i].nFenceId);
										pFence->nFenceType = pSqlFenceList[i].usFenceType;
										strncpy_s(pFence->szFenceContent, sizeof(pFence->szFenceContent),
											pSqlFenceList[i].szFenceContent, strlen(pSqlFenceList[i].szFenceContent));
										pFence->nActiveFlag = pSqlFenceList[i].nFenceActive;
										pFence->nCoordinate = pSqlFenceList[i].nCoordinate;
										zhash_update(g_fenceList, pFence->szFenceId, pFence);
										zhash_freefn(g_fenceList, pFence->szFenceId, free);
										memcpy_s(bufferUpdateInfo.pUpdateData + nOffset, bufferUpdateInfo.uiUpdateDataLen
											- nOffset, pFence, nFenceSize);
										nOffset += nFenceSize;
										snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update from DB, INSERT new "
											"fence id=%d, type=%d, active=%d, content=%s, coordinate=%d\r\n", __FUNCTION__, __LINE__,
											pSqlFenceList[i].nFenceId, pSqlFenceList[i].usFenceType,
											pSqlFenceList[i].nFenceActive, pSqlFenceList[i].szFenceContent,
											pSqlFenceList[i].nCoordinate);
										LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
									}
								}
								pthread_mutex_unlock(&g_mutex4FenceList);
								bufferUpdateInfo.pUpdateData[bufferUpdateInfo.uiUpdateDataLen] = '\0';
								bufferUpdateInfo.ulUpdateTime = (unsigned long long)time(NULL);
								size_t nBufferUpdateSize = sizeof(BufferUpdate);
								unsigned int uiUpdatePipeSequence = getNextPipeSequence();
								MessagePayload msgPayload;
								size_t nPayloadSize = sizeof(MessagePayload);
								MAKE_PAYLOAD_MARK(msgPayload.szMsgMark);
								MAKE_PAYLOAD_VERSION(msgPayload.szMsgVersion);
								msgPayload.uiMsgType = MSG_BUFFER_MODIFY;
								msgPayload.uiMsgSequence = uiUpdatePipeSequence;
								msgPayload.uiMsgCount = 1;
								msgPayload.uiMsgDataLength = (unsigned int)nBufferUpdateSize + bufferUpdateInfo.uiUpdateDataLen;
								msgPayload.pMsgData = (unsigned char *)zmalloc(msgPayload.uiMsgDataLength + 1);
								memcpy_s(msgPayload.pMsgData, msgPayload.uiMsgDataLength, &bufferUpdateInfo,
									nBufferUpdateSize);
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

								free(pSqlFenceList);
								pSqlFenceList = NULL;
								if (bufferUpdateInfo.pUpdateData && bufferUpdateInfo.uiUpdateDataLen > 0) {
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
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update fenceList sql at %s\r\n",
									__FUNCTION__, __LINE__, szLastUpdateTime);
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
							}
							mysql_free_result(res_ptr);
						}
					}
					else {
						snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]execute update FenceList sql at %s failed, "
							"error=%d,%s\r\n", __FUNCTION__, __LINE__, szLastUpdateTime, mysql_errno(m_updateConn),
							mysql_error(m_updateConn));
						LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_FAULT, m_usLogType);
						if (nErr == 2013 || nErr == 2006) {
							mysql_close(m_updateConn);
							m_updateConn = NULL;
							m_updateConn = mysql_init(NULL);
							if (m_updateConn && mysql_real_connect(m_updateConn, m_zkDbProxy.szDbHostIp, m_zkDbProxy.szDbUser,
								m_zkDbProxy.szDbPasswd, m_zkDbProxy.szMajorSample, m_zkDbProxy.usMasterDbPort, NULL, 0)) {
								snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect update db %s, ip=%s, port=%hu, user=%s "
									"at %llu\r\n", __FUNCTION__, __LINE__, m_zkDbProxy.szMajorSample, m_zkDbProxy.szDbHostIp,
									m_zkDbProxy.usMasterDbPort, m_zkDbProxy.szDbUser, (unsigned long long)time(NULL));
								LOG_Log(m_ullLogInst, szLog, pf_logger::eLOGCATEGORY_INFO, m_usLogType);
								mysql_set_character_set(m_updateConn, "gb2312");
							}
						}
					}

					pthread_mutex_unlock(&m_mutex4UpdateConn);
				}
			} while (0);
			g_ulLastUpdateTime = pTask->ulUpdateTaskTime;
			setPipeState(dbproxy::E_PIPE_CLOSE);
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

int DbProxy::getPipeState()
{
	int result = 0;
	pthread_mutex_lock(&g_mutex4PipeState);
	result = g_nUpdatePipeState;
	pthread_mutex_unlock(&g_mutex4PipeState);
	return result;
}

void DbProxy::setPipeState(int nState_)
{
	pthread_mutex_lock(&g_mutex4PipeState);
	g_nUpdatePipeState = nState_;
	pthread_mutex_unlock(&g_mutex4PipeState);
}



int supervise(zloop_t * loop_, int timer_id_, void * arg_)
{
	int result = 0;
	DbProxy * pProxy = (DbProxy *)arg_;
	if (pProxy) {
		if (!pProxy->m_nRun) {
			zloop_reader_end(loop_, pProxy->m_subscriber);
			zloop_reader_end(loop_, pProxy->m_reception);
			zloop_reader_end(loop_, pProxy->m_pipeline);
			zloop_reader_end(loop_, pProxy->m_interactor);
			zloop_timer_end(loop_, timer_id_);
			result = -1;
		}
		else {
			if (pProxy->m_nTimerTickCount % 60 == 0) { //1.0 min
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
			if (pProxy->m_nTimerTickCount % 30 == 0) { //30sec
				time_t now = time(NULL);
				pthread_mutex_lock(&DbProxy::g_mutex4UpdateTime);
				double interval = difftime(now, (time_t)DbProxy::g_ulLastUpdateTime);
				if (interval >= 30.00/*120.0000*/) {
					int nState = pProxy->getPipeState();
					if (nState == dbproxy::E_PIPE_CLOSE) {
						size_t nTaskSize = sizeof(dbproxy::UpdatePipeTask);
						dbproxy::UpdatePipeTask * pTask = (dbproxy::UpdatePipeTask *)zmalloc(nTaskSize);
						pTask->ulUpdateTaskTime = (unsigned long)now;
						if (!pProxy->addUpdateTask(pTask)) {
							free(pTask);
							pTask = NULL;
						}
						else {
							pProxy->setPipeState(dbproxy::E_PIPE_OPEN);
						}
					}
					else if (interval >= 90) {
						size_t nTaskSize = sizeof(dbproxy::UpdatePipeTask);
						dbproxy::UpdatePipeTask * pTask = (dbproxy::UpdatePipeTask *)zmalloc(nTaskSize);
						pTask->ulUpdateTaskTime = (unsigned long)now;
						if (!pProxy->addUpdateTask(pTask)) {
							free(pTask);
							pTask = NULL;
						}
						else {
							pProxy->setPipeState(dbproxy::E_PIPE_OPEN);
						}
					}
				}
				pthread_mutex_unlock(&DbProxy::g_mutex4UpdateTime);
			}
			if (pProxy->m_nTimerTickCount % 300 == 0) { //5min
				//keep connection alive
				char szLog[512] = { 0 };
				pthread_mutex_lock(&pProxy->m_mutex4LocateConn);
				if (pProxy->m_locateConn) {
					if (mysql_ping(pProxy->m_locateConn) != 0) {
						//connection exception
						mysql_close(pProxy->m_locateConn);
						pProxy->m_locateConn = NULL;
						//re-connection
						pProxy->m_locateConn = mysql_init(NULL);
						if (pProxy->m_locateConn && mysql_real_connect(pProxy->m_locateConn, pProxy->m_zkDbProxy.szDbHostIp, 
							pProxy->m_zkDbProxy.szDbUser, pProxy->m_zkDbProxy.szDbPasswd, pProxy->m_zkDbProxy.szLocateSample,
							pProxy->m_zkDbProxy.usMasterDbPort, NULL, 0)) {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect locate db %s, ip=%s, port=%hu, user=%s at %ld\r\n", 
								__FUNCTION__, __LINE__, pProxy->m_zkDbProxy.szLocateSample, pProxy->m_zkDbProxy.szDbHostIp, 
								pProxy->m_zkDbProxy.usMasterDbPort, pProxy->m_zkDbProxy.szDbUser, (long)time(NULL));
							LOG_Log(pProxy->m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, pProxy->m_usLogType);
							mysql_set_character_set(pProxy->m_locateConn, "gb2312");
						}
					}
				}
				pthread_mutex_unlock(&pProxy->m_mutex4LocateConn);

				pthread_mutex_lock(&pProxy->m_mutex4ReadConn);
				if (pProxy->m_readConn) {
					if (mysql_ping(pProxy->m_readConn) != 0) {
						mysql_close(pProxy->m_readConn);
						pProxy->m_readConn = NULL;
						//re-connection
						pProxy->m_readConn = mysql_init(NULL);
						if (pProxy->m_readConn && mysql_real_connect(pProxy->m_readConn, pProxy->m_zkDbProxy.szSlaveDbHostIp,
							pProxy->m_zkDbProxy.szSlaveDbUser, pProxy->m_zkDbProxy.szSlaveDbPasswd, pProxy->m_zkDbProxy.szMajorSample,
							pProxy->m_zkDbProxy.usSlaveDbPort, NULL, 0)) {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect read db %s, ip=%s, port=%hu, user=%s at %ld\r\n",
								__FUNCTION__, __LINE__, pProxy->m_zkDbProxy.szMajorSample, pProxy->m_zkDbProxy.szDbHostIp,
								pProxy->m_zkDbProxy.usSlaveDbPort, pProxy->m_zkDbProxy.szDbUser, (long)time(NULL));
							LOG_Log(pProxy->m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, pProxy->m_usLogType);
							mysql_set_character_set(pProxy->m_readConn, "gb2312");
						}
					}
				}
				pthread_mutex_unlock(&pProxy->m_mutex4ReadConn);

				pthread_mutex_lock(&pProxy->m_mutex4WriteConn);
				if (pProxy->m_writeConn) {
					if (mysql_ping(pProxy->m_writeConn) != 0) {
						mysql_close(pProxy->m_writeConn);
						pProxy->m_writeConn = NULL;
						//re-connection
						pProxy->m_writeConn = mysql_init(NULL);
						if (pProxy->m_writeConn && mysql_real_connect(pProxy->m_writeConn, pProxy->m_zkDbProxy.szDbHostIp,
							pProxy->m_zkDbProxy.szDbUser, pProxy->m_zkDbProxy.szDbPasswd, pProxy->m_zkDbProxy.szMajorSample,
							pProxy->m_zkDbProxy.usMasterDbPort, NULL, 0)) {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect write db %s, ip=%s, port=%hu, user=%s at %ld\r\n",
								__FUNCTION__, __LINE__, pProxy->m_zkDbProxy.szMajorSample, pProxy->m_zkDbProxy.szDbHostIp,
								pProxy->m_zkDbProxy.usMasterDbPort, pProxy->m_zkDbProxy.szDbUser, (long)time(NULL));
							LOG_Log(pProxy->m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, pProxy->m_usLogType);
							mysql_set_character_set(pProxy->m_writeConn, "gb2312");
						}
					}
				}
				pthread_mutex_unlock(&pProxy->m_mutex4WriteConn);

				pthread_mutex_lock(&pProxy->m_mutex4UpdateConn);
				if (pProxy->m_updateConn) {
					if (mysql_ping(pProxy->m_updateConn) != 0) {
						mysql_close(pProxy->m_updateConn);
						pProxy->m_updateConn = NULL;
						//re-connection
						pProxy->m_updateConn = mysql_init(NULL);
						if (pProxy->m_updateConn && mysql_real_connect(pProxy->m_updateConn, pProxy->m_zkDbProxy.szDbHostIp,
							pProxy->m_zkDbProxy.szDbUser, pProxy->m_zkDbProxy.szDbPasswd, pProxy->m_zkDbProxy.szMajorSample,
							pProxy->m_zkDbProxy.usMasterDbPort, NULL, 0)) {
							snprintf(szLog, sizeof(szLog), "[DbProxy]%s[%d]re-connect update db %s, ip=%s, port=%hu, user=%s at %ld\r\n",
								__FUNCTION__, __LINE__, pProxy->m_zkDbProxy.szMajorSample, pProxy->m_zkDbProxy.szDbHostIp,
								pProxy->m_zkDbProxy.usMasterDbPort, pProxy->m_zkDbProxy.szDbUser, (long)time(NULL));
							LOG_Log(pProxy->m_ullLogInst, szLog,pf_logger::eLOGCATEGORY_INFO, pProxy->m_usLogType);
							mysql_set_character_set(pProxy->m_updateConn, "gb2312");
						}
					}
				}
				pthread_mutex_unlock(&pProxy->m_mutex4UpdateConn);
			}
			pProxy->m_nTimerTickCount++;
			if (pProxy->m_nTimerTickCount == 72001) {
				pProxy->m_nTimerTickCount = 1;
			}
			Sleep(50);
		}
	}
	return result;
}

int readSubscriber(zloop_t * loop_, zsock_t * reader_, void * arg_)
{
	auto pInst = (DbProxy *)arg_;
	if (pInst) {
		if (pInst->m_nRun) {
			zmsg_t * msg;
			zsock_recv(reader_, "m", &msg);
			if (msg) {
				if (zmsg_size(msg) >= 6) {
					zframe_t * frame_mark = zmsg_pop(msg);
					zframe_t * frame_seq = zmsg_pop(msg);
					zframe_t * frame_type = zmsg_pop(msg);
					zframe_t * frame_uuid = zmsg_pop(msg);
					zframe_t * frame_body = zmsg_pop(msg);
					zframe_t * frame_from = zmsg_pop(msg);
					char szMark[64] = { 0 };
					memcpy_s(szMark, sizeof(szMark), zframe_data(frame_mark), zframe_size(frame_mark));
					char szSeq[20] = { 0 };
					memcpy_s(szSeq, sizeof(szSeq), zframe_data(frame_seq), zframe_size(frame_seq));
					char szType[16] = { 0 };
					memcpy_s(szType, sizeof(szType), zframe_data(frame_type), zframe_size(frame_type));
					char szUuid[64] = { 0 };
					memcpy_s(szUuid, sizeof(szUuid), zframe_data(frame_uuid), zframe_size(frame_uuid));
					char szBody[512] = { 0 };
					memcpy_s(szBody, sizeof(szBody), zframe_data(frame_body), zframe_size(frame_body));
					char szFrom[64] = { 0 };
					memcpy_s(szFrom, sizeof(szFrom), zframe_data(frame_from), zframe_size(frame_from));
					TopicMessage * pMsg = (TopicMessage *)zmalloc(sizeof(TopicMessage));
					if (pMsg) {
						strncpy_s(pMsg->szMsgMark, sizeof(pMsg->szMsgMark), szMark, strlen(szMark));
						strncpy_s(pMsg->szMsgUuid, sizeof(pMsg->szMsgUuid), szUuid, strlen(szUuid));
						strncpy_s(pMsg->szMsgBody, sizeof(pMsg->szMsgBody), szBody, strlen(szBody));
						strncpy_s(pMsg->szMsgFrom, sizeof(pMsg->szMsgFrom), szFrom, strlen(szFrom));
						pMsg->uiMsgSequence = (unsigned int)atoi(szSeq);
						pMsg->uiMsgType = (unsigned int)atoi(szType);
						if (!pInst->addTopicMsg(pMsg)) {
							free(pMsg);
							pMsg = NULL;
						}
					}
					zframe_destroy(&frame_mark);
					zframe_destroy(&frame_seq);
					zframe_destroy(&frame_type);
					zframe_destroy(&frame_uuid);
					zframe_destroy(&frame_body);
					zframe_destroy(&frame_from);
				}
				zmsg_destroy(&msg);
			}
		}
		else {
			return -1;
		}
	}
	return 0;
}

int readInteractor(zloop_t * loop_, zsock_t * reader_, void * arg_)
{
	auto pInst = (DbProxy *)arg_;
	if (pInst) {
		if (pInst->m_nRun) {
			zmsg_t * interactMsg;
			zsock_recv(reader_, "m", &interactMsg);
			if (interactMsg) {
				size_t nCount = zmsg_size(interactMsg);
				if (nCount) {
					zframe_t ** interactFrames = (zframe_t **)zmalloc(nCount * sizeof(zframe_t *));
					InteractionMessage * pMsg = (InteractionMessage *)zmalloc(sizeof(InteractionMessage));
					pMsg->uiContentCount = (unsigned int)nCount;
					pMsg->pMsgContents = (char **)zmalloc(nCount * sizeof(char *));
					pMsg->uiContentLens = (unsigned int *)zmalloc(nCount * sizeof(unsigned int));
					for (size_t i = 0; i < nCount; i++) {
						interactFrames[i] = zmsg_pop(interactMsg);
						size_t nFrameLen = zframe_size(interactFrames[i]);
						pMsg->uiContentLens[i] = (unsigned int)nFrameLen;
						pMsg->pMsgContents[i] = (char *)zmalloc(nFrameLen + 1);
						memcpy_s(pMsg->pMsgContents[i], nFrameLen + 1, zframe_data(interactFrames[i]), nFrameLen);
						pMsg->pMsgContents[i][nFrameLen] = '\0';
						zframe_destroy(&interactFrames[i]);
					}
					if (!pInst->addInteractMsg(pMsg)) {
						for (size_t i = 0; i < nCount; i++) {
							if (pMsg->pMsgContents[i]) {
								free(pMsg->pMsgContents[i]);
								pMsg->pMsgContents[i] = NULL;
							}
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
		else {
			return -1;
		}
	}
	return 0;
}

int readReception(zloop_t * loop_, zsock_t * reader_, void * arg_)
{
	auto pInst = (DbProxy *)arg_;
	if (pInst) {
		if (pInst->m_nRun) {
			zmsg_t * receptMsg;
			zsock_recv(reader_, "m", &receptMsg);
			if (receptMsg) {
				size_t nFrameSize = zmsg_size(receptMsg);
				zframe_t * frame_identity = zmsg_pop(receptMsg);
				char szIdentity[64] = { 0 };
				if (zframe_size(frame_identity)) {
					memcpy_s(szIdentity, sizeof(szIdentity), zframe_data(frame_identity), zframe_size(frame_identity));
				}
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
						memcpy_s(container.pStoreResult, container.uiResultLen, pFrameData + nContainerSize, container.uiResultLen);
						container.pStoreResult[container.uiResultLen] = '\0';
					}
					pInst->handleReception(&container, szIdentity);
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
		else {
			return -1;
		}
	}
	return 0;
}

int readPipeline(zloop_t * loop_, zsock_t * reader_, void * arg_)
{
	auto pInst = (DbProxy *)arg_;
	if (pInst) {
		if (pInst->m_nRun) {
			zmsg_t * msg;
			zsock_recv(reader_, "m", &msg);
			if (msg) {

				zmsg_destroy(&msg);
			}
		}
		else {
			return -1;
		}
	}
	return 0;
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

