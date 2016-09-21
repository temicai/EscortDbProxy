#ifndef DBPROXYCONCRETE_H
#define DBPROXYCONCRETE_H

#include <queue>
#include <time.h>
#include "mysql.h"
#define _TIMESPEC_DEFINED
#include "pthread.h"
#include "czmq.h"
#include "zmq.h"
#include "zookeeper.h"
#include "escort_common.h"
#include "escort_error.h"
#include "zk_escort.h"
#include "pfLog.hh"

#pragma comment(lib, "libmysql.lib")
#pragma comment(lib, "pthreadVC2.lib")
#pragma comment(lib, "pfLog.lib")
#pragma comment(lib, "libzmq.lib")
#pragma comment(lib, "czmq.lib")
#ifdef _DEBUG
#pragma comment(lib, "zookeeper_d.lib")
#else 
#pragma comment(lib, "zookeeper.lib")
#endif

namespace dbproxy
{
	typedef struct tagSqlStatement
	{
		char * pStatement;
		unsigned int uiStatementLen;
	} SqlStatement;

	typedef struct tagLogContext
	{
		char * pLogData;
		unsigned int uiDataLen;
		int nLogCategory;
		int nLogType;
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
}

class DbProxy
{
public:
	DbProxy();
	~DbProxy();

private:
	int m_nRun;
	//mq
	zctx_t * m_ctx;
	void * m_reception; //ROUTER
	void * m_subscriber; 
	pthread_t m_pthdNetwork;

	//zookeeper
	zhandle_t * m_zkHandle;
	char m_szZkHost[256];
	char m_zkNodePath[256];
	bool m_bZKConnected;
	ZkDatabaseProxy m_zkDbProx;
	
	//mysql
	MYSQL * m_writeConn;
	MYSQL * m_readConn;
	std::queue<dbproxy::SqlStatement *> m_sqlQueryList;
	std::queue<dbproxy::SqlStatement *> m_sqlExecuteList;
	pthread_mutex_t m_mutex4QryList;
	pthread_mutex_t m_mutex4ExecList;
	pthread_cond_t m_cond4QryList;
	pthread_cond_t m_cond4ExecList;
	pthread_t m_pthdQuery;
	pthread_t m_pthdExec;

	//buffer
	static zhash_t * g_deviceList;
	static zhash_t * g_guarderList;
	static zhash_t * g_taskList;
	static pthread_mutex_t g_mutex4DevList;
	static pthread_mutex_t g_mutex4GuarderList;
	static pthread_mutex_t g_mutex4TaskList;
	static unsigned int g_uiRefCount;
	static unsigned short g_usSequence;

	//log
	long m_nLogInst;
	long m_nLogType;
	char m_szLogRoot[256];
	pthread_t m_pthdLog;
	std::queue<dbproxy::LogContext *> m_logQue;
	pthread_mutex_t m_mutex4LogQue;
	pthread_cond_t m_cond4LogQue;

protected:
	void initLog();
	bool addLog(dbproxy::LogContext * pLog);
	void writeLog(const char * pLogContent, int nLogCategoryType, int nLogType);
	void dealLog();





	friend void * dealLogThread(void *);
	friend void * dealSqlQueryThread(void *);
	friend void * dealSqlExecThread(void *);
	friend void * dealNetworkThread(void *);
};




#endif 
