#ifndef ESCORTDBCOMMON_H
#define ESCORTDBCOMMON_H

#define DBNAME_MAJOR "escort"
#define DBNAME_LOCATE "escort_locate"

namespace escort_db
{
	const int ROLE_ADMIN = 1;
	const int ROLE_NORMAL = 2;

	enum eEscortDBOperate
	{
		E_OPT_QUERY = 0,
		E_OPT_INSERT = 1,
		E_OPT_UPDATE = 2,
		E_OPT_DELETE = 3,
	};

	enum eEsortDBTable
	{
		E_TBL_UNDEFINE = 0,
		E_TBL_PERSON = 1,		//person_info
		E_TBL_GUARDER = 2,	//user_info
		E_TBL_DEVICE = 3,		//device_info		
		E_TBL_TASK = 4,			//task_info	
		E_TBL_ALARM = 5,		//alarm_info
		E_TBL_MESSAGE = 6,	//message_info	
		E_TBL_ORG = 7,			//org_info
		E_TBL_LOCATE = 8,		
	};

	enum eEscortDBAlarmType
	{
		E_ALMTYPE_OFFLINE = 0,
		E_ALMTYPE_FLEE = 1,
		E_ALMTYPE_LOWPOWER = 2,
		E_ALMTYPE_LOOSE = 3,
	};

	enum eEscortDBLocateType
	{
		E_LOCATE_GPS = 0,
		E_LOCATE_LBS = 1,
		E_LOCATE_WIFI = 2,
		E_LOCATE_APP = 3,
	};

	typedef struct tagSqlContainer
	{
		unsigned int uiSqlOptSeq;
		unsigned long ulSqlOptTime;
		unsigned short usSqlOptTarget;	//db-table
		unsigned short usSqlOptType;		//dbOperate
		char szSqlOptKey[64];
		unsigned int uiResultCount;
		unsigned int uiResultLen;
		unsigned char * pStoreResult;
	} SqlContainer;

	typedef struct tagSqlPerson
	{
		char szPersonId[20];
		char szPersonName[32];
		int nFlee;
		tagSqlPerson()
		{
			szPersonId[0] = '\0';
			szPersonName[0] = '\0';
			nFlee = 0;
		}
	} SqlPerson;

	typedef struct tagSqlGuarder
	{
		char szUserId[20];
		char szUserName[32];
		char szPasswd[64];
		char szOrgId[40];
		tagSqlGuarder()
		{
			szUserId[0] = '\0';
			szUserName[0] = '\0';
			szPasswd[0] = '\0';
			szOrgId[0] = '\0';
		}
	} SqlGuarder;

	typedef struct tagSqlTask
	{
		char szTaskId[20];
		unsigned short usTaskType;
		unsigned short usTaskLimit;
		char szDestination[32];
		char szStartTime[20];
		char szGuarderId[20];
		char szDeviceId[16];
		SqlPerson person;
		int nFleeFlag;
		tagSqlTask()
		{
			szTaskId[0] = '\0';
			szDestination[0] = '\0';
			szStartTime[0] = '\0';
			szGuarderId[0] = '\0';
			szDeviceId[0] = '\0';
			nFleeFlag = 0;
		}
	} SqlTask;

	typedef struct tagSqlDevice
	{
		char szDeviceId[16];
		char szFactoryId[4];
		char szOrgId[40];
		char szLastCommuncation[20];
		char szLastLocation[20];
		double dLat;
		double dLng;
		int nLocationType;
		unsigned short usIsUse;
		unsigned short usBattery;
		unsigned short usOnline;
		unsigned short usIsRemove;
		tagSqlDevice()
		{
			szDeviceId[0] = '\0';
			szFactoryId[0] = '\0';
			szOrgId[0] = '\0';
			szLastCommuncation[0] = '\0';
			szLastLocation[0] = '\0';
			dLat = dLng = 0.000000;
		}
	} SqlDevice;

	typedef struct tagSqlOrg
	{
		char szOrgId[40];
		char szOrgName[40];
		char szParentOrgId[40];
		tagSqlOrg()
		{
			szOrgId[0] = '\0';
			szOrgName[0] = '\0';
			szParentOrgId[0] = '\0';
		}
	} SqlOrg;

	typedef struct tagSqlMessage
	{
		char szMsgTopic[64];
		unsigned short usMsgType;
		unsigned short usMsgSeq;
		char szMsgUuid[40];
		char szMsgBody[256];
		unsigned long ulMsgTime;
		tagSqlMessage()
		{
			szMsgTopic[0] = '\0';
			szMsgUuid[0] = '\0';
			szMsgBody[0] = '\0';
			usMsgType = 0;
			usMsgSeq = 0;
			ulMsgTime = 0;
		}
	} SqlMessage;

}




#endif
