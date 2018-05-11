#ifndef DBPROXYCONCRETE_H
#define DBPROXYCONCRETE_H

#include <WinSock2.h>
#include <queue>
#include <time.h>
#include <string>
#include "mysql.h"
#define _TIMESPEC_DEFINED
#include "pthread.h"
#include "czmq.h"
#include "zmq.h"
#include "zookeeper.h"
#include "escort_common.h"
#include "escort_error.h"
#include "zk_escort.h"
#include "pf_log.h"
#include "document.h" //rapidjson
#include "EscortDbCommon.h"

#pragma comment(lib, "ws2_32.lib")
#pragma comment(lib, "libmysql.lib")
#pragma comment(lib, "pthreadVC2.lib")
#pragma comment(lib, "pf_log.lib")
#pragma comment(lib, "libzmq.lib")
#pragma comment(lib, "czmq.lib")
#pragma comment(lib, "zookeeper.lib")


#define SQLTYPE_QUERY 0
#define SQLTYPE_EXECUTE 1
#define SQLTYPE_OTHER 2

namespace dbproxy
{
	typedef struct tagSqlStatement
	{
		char * pStatement;
		unsigned int uiStatementLen;
		unsigned int uiCorrelativeTable;
		tagSqlStatement()
		{
			pStatement = NULL;
			uiStatementLen = 0;
			uiCorrelativeTable = 0;
		}
		~tagSqlStatement()
		{
			if (pStatement) {
				free(pStatement);
				pStatement = NULL;
				uiStatementLen = 0;
			}
		}
	} SqlStatement;

	typedef struct tagSqlTransaction
	{
		unsigned long long ulTransactionTime;
		unsigned int uiTransactionSequence;
		unsigned int uiSqlCount;
		SqlStatement * pSqlList;
		char szTransactionFrom[40];
		tagSqlTransaction()
		{
			uiTransactionSequence = 0;
			ulTransactionTime = 0;
			uiSqlCount = 0;
			pSqlList = NULL;
			szTransactionFrom[0] = '\0';
		}
		~tagSqlTransaction()
		{
			if (pSqlList) {
				free(pSqlList);
				pSqlList = NULL;
			}
		}
	} SqlTransaction;

	typedef struct tagLogContext
	{
		char * pLogData;
		unsigned int uiDataLen;
		unsigned short nLogCategory;
		unsigned short nLogType;
		tagLogContext()
		{
			uiDataLen = 0;
			pLogData = NULL;
			nLogCategory = 0;
			nLogType = 0;
		}
		~tagLogContext()
		{
			if (uiDataLen && pLogData) {
				free(pLogData);
				pLogData = NULL;
				uiDataLen = 0;
			}
		}
	} LogContext;

	typedef struct tagRemoteLinkInfo
	{
		int nActive;
		unsigned long long ulLastActiveTime;
	} RemoteLinkInfo;

	typedef struct tagUpdatePipeTask
	{
		unsigned long long ulUpdateTaskTime;
	} UpdatePipeTask;

	enum eUpdatePipeState
	{
		E_PIPE_CLOSE = 0,
		E_PIPE_OPEN = 1,
		E_PIPE_SPOUT = 2,
	};

}

using namespace escort;

class DbProxy
{
public:
	DbProxy(const char * pZkHost, const char * pRoot);
	~DbProxy();
	int Start(const char * pHost, unsigned short usReceptPort, const char * pMidwareHost, unsigned short usPublishPort,
		unsigned short usContactPort, unsigned short usCollectPort, const char * pMasterDbHost, const char * pMasterDbUser,
		const char * pMasterDbPasswd, unsigned short usMasterDbPort, const char * pSlaveDbHost, const char * pSlaveDbUser,
		const char * pSlaveDbPasswd, unsigned short usSlaveDbPort, const char * pDataSample, const char * pDataSample2);
	int Stop();
	int GetState();
	void SetLogType(unsigned short usLogType);
private:
	int m_nRun;
	//mq
	zsock_t * m_reception; //ROUTER
	zsock_t * m_subscriber; 
	zsock_t * m_interactor; 
	zsock_t * m_pipeline;  //collect

	pthread_t m_pthdNetwork;
	char m_szPipelineIdentity[40];

	//zookeeper
	zhandle_t * m_zkHandle;
	char m_szZkHost[256];
	char m_zkNodePath[256];
	bool m_bZKConnected;
	ZkDatabaseProxy m_zkDbProxy;
	
	//mysql
	MYSQL * m_writeConn;
	MYSQL * m_readConn;
	MYSQL * m_locateConn; //escort locate
	MYSQL * m_updateConn;
	pthread_mutex_t m_mutex4ReadConn;
	pthread_mutex_t m_mutex4WriteConn;
	pthread_mutex_t m_mutex4LocateConn;
	pthread_mutex_t m_mutex4UpdateConn;

	std::queue<dbproxy::SqlTransaction *> m_sqlQueryQue;
	std::queue<dbproxy::SqlTransaction *> m_sqlExecuteQue;
	std::queue<dbproxy::SqlTransaction *> m_sqlLocateQue;  //escort_locate
	pthread_mutex_t m_mutex4QryQue;
	pthread_mutex_t m_mutex4ExecQue;
	pthread_mutex_t m_mutex4LocateQue;
	pthread_cond_t m_cond4QryQue;
	pthread_cond_t m_cond4ExecQue;
	pthread_cond_t m_cond4LocateQue;
	pthread_t m_pthdQuery;
	pthread_t m_pthdExec;
	pthread_t m_pthdLocate;

	//buffer
	static zhash_t * g_deviceList;
	static zhash_t * g_guarderList;
	static zhash_t * g_taskList;
	static zhash_t * g_personList;
	static zhash_t * g_orgList;
	static zhash_t * g_fenceList;
	static zhash_t * g_fenceTaskList;
	static pthread_mutex_t g_mutex4DevList;
	static pthread_mutex_t g_mutex4GuarderList;
	static pthread_mutex_t g_mutex4TaskList;
	static pthread_mutex_t g_mutex4PersonList;
	static pthread_mutex_t g_mutex4OrgList;
	static pthread_mutex_t g_mutex4FenceList;
	static pthread_mutex_t g_mutex4FenceTaskList;
	static int g_nRefCount;
	static unsigned int g_uiInteractSequence;
	static pthread_mutex_t g_mutex4InteractSequence;
	static unsigned int g_uiPipeSequence;
	static pthread_mutex_t g_mutex4PipeSequence;
	static BOOL g_bLoadSql;
	//static char g_szLastUpdateTime[20]; 
	static int g_nUpdatePipeState; //0, 1
	static pthread_mutex_t g_mutex4PipeState;
	static unsigned long long g_ulLastUpdateTime;
	static pthread_mutex_t g_mutex4UpdateTime;

	pthread_mutex_t m_mutex4UpdatePipe; 
	pthread_cond_t m_cond4UpdatePipe; 
	std::queue<dbproxy::UpdatePipeTask *> m_updateTaskQue;
	pthread_t m_pthdUpdatePipe; //

	//log
	unsigned long long m_ullLogInst;
	unsigned short m_usLogType;
	char m_szLogRoot[256];

	//topicMessage
	std::queue<TopicMessage *> m_topicMsgQue;
	pthread_mutex_t m_mutex4TopicMsgQue;
	pthread_cond_t m_cond4TopicMsgQue;
	pthread_t m_pthdTopicMsg;
	std::queue<InteractionMessage *> m_interactMsgQue;
	pthread_mutex_t m_mutex4InteractMsgQueu;
	pthread_cond_t m_cond4InteractMsgQue;
	pthread_t m_pthdInteractMsg;

	dbproxy::RemoteLinkInfo m_remoteLink;
	pthread_mutex_t m_mutex4RemoteLink;

	pthread_t m_pthdSupervise;
	zloop_t * m_loop;
	int m_nTimer4Supervise;
	int m_nTimerTickCount;
	
protected:
	void initLog();

	bool addSqlTransaction(dbproxy::SqlTransaction *, int);
	void dealSqlQuery();
	void dealSqlExec();
	void dealSqlLocate();
	void handleSqlExec(dbproxy::SqlStatement *, unsigned int, unsigned long long, const char *);
	void handleSqlQry(const dbproxy::SqlStatement *, unsigned int, unsigned long long, const char *);
	void handleSqlLocate(dbproxy::SqlStatement *, unsigned int, unsigned long long);
	void replyQuery(void *, unsigned int, unsigned int, unsigned int, unsigned long long, const char *);

	void dealNetwork();
	bool addTopicMsg(TopicMessage * pMsg);
	void dealTopicMsg();
	void storeTopicMsg(TopicMessage * pMsg, unsigned long long);
	int handleTopicDeviceAliveMsg(TopicAliveMessage *);
	int handleTopicDeviceOnlineMsg(TopicOnlineMessage *);
	int handleTopicDeviceOfflineMsg(TopicOfflineMessage *);
	int handleTopicBindMsg(TopicBindMessage *);
	int handleTopicTaskSubmitMsg(TopicTaskMessage *);
	int handleTopicTaskCloseMsg(TopicTaskCloseMessage *);
	int handleTopicTaskModifyMsg(TopicTaskModifyMessage *);
	int handleTopicGpsLocateMsg(TopicLocateMessageGps *);
	int handleTopicLbsLocateMsg(TopicLocateMessageLbs *);
	int handleTopicAppLocateMsg(TopicLocateMessageApp *);
	int handleTopicLowpoweAlarmMsg(TopicAlarmMessageLowpower *);
	int handleTopicLooseAlarmMsg(TopicAlarmMessageLoose *);
	int handleTopicFleeAlarmMsg(TopicAlarmMessageFlee *);
	int handleTopicFenceAlarmMsg(TopicAlarmMessageFence *);
	int handleTopicLocateLostAlarmMsg(TopicAlarmMessageLocateLost *);
	int handleTopicPeerFenceAlarmMsg(TopicAlarmMessagePeerFence *);
	int handleTopicDeviceChargeMsg(TopicDeviceChargeMessage *);

	bool addInteractMsg(InteractionMessage *);
	void dealInteractMsg();

	void handleReception(escort_db::SqlContainer *, const char *);
	bool makePerson(const char *, Person *);
	void changeDeviceStatus(unsigned short usNewStatus, unsigned short & usDeviceStatus, int nMode = 0);
	unsigned int getNextInteractSequence();
	unsigned int getNextPipeSequence();

	void initZookeeper();
	int competeForMaster();
	void masterExist();
	int runAsSlaver();
	void removeSlaver();
	int setZkDbProxyData(const char * pPath, void * pData, size_t nDataSize);
	int getZkMidwareData(const char * pPath, ZkMidware * pData);

	int sendDataViaInteractor(const char *, size_t);
	bool initSqlBuffer();
	void updatePipeLoop();
	bool addUpdateTask(dbproxy::UpdatePipeTask * pUpdateTask);
	int getPipeState();
	void setPipeState(int);
	
	friend int supervise(zloop_t *, int, void *);
	friend int readSubscriber(zloop_t *, zsock_t * reader_, void *);
	friend int readPipeline(zloop_t *, zsock_t * reader_, void *);
	friend int readInteractor(zloop_t *, zsock_t *, void *);
	friend int readReception(zloop_t *, zsock_t *, void *);

	//friend void * dealLogThread(void *);
	friend void * dealSqlQueryThread(void *);
	friend void * dealSqlExecThread(void *);
	friend void * dealSqlLocateThread(void *);
	friend void * dealNetworkThread(void *);
	friend void * dealTopicMsgThread(void *);
	friend void * dealInteractMsgThread(void *);
	friend void * superviseThread(void *);
	friend void * dealUpdatePipeThread(void *);

	friend void zk_server_watcher(zhandle_t *, int, int, const char *, void *);
	friend void zk_escort_create_completion(int, const char *, const void *);
	friend void zk_dbproxy_create_completion(int, const char *, const void *);
	friend void zk_dbproxy_master_create_completion(int, const char *, const void *);
	friend void zk_dbproxy_master_exists_watcher(zhandle_t *, int, int, const char * , void *);
	friend void zk_dbproxy_master_exists_completion(int, const Stat *, const void *);
	friend void zk_dbproxy_slaver_create_completion(int, const char *, const void *);
	friend void zk_dbproxy_set_completion(int, const Stat *, const void *);



};




#endif 
