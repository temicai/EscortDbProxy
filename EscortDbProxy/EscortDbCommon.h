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

	enum eEscortDBKeyQueryDescription
	{
		E_KEY_EQUAL = 0,    // = key
		E_KEY_NOT_EQUAL = 1, // != key
		E_KEY_LIKE_FORE = 2, // %key
		E_KEY_LIEK_TAIL = 3, // key%
		E_KEY_LIKE_FORETAIL = 4, //%key%
	};

	enum eEsortDBTable
	{
		E_TBL_UNDEFINE = 0,
		E_TBL_PERSON = 1,				//person_info
		E_TBL_GUARDER = 2,			//user_info
		E_TBL_DEVICE = 3,				//device_info
		E_TBL_TASK = 4,					//task_info
		E_TBL_ALARM = 5,				//alarm_info
		E_TBL_MESSAGE = 6,			//message_info
		E_TBL_ORG = 7,					//org_info
		E_TBL_LOCATE = 8,				
		E_TBL_TASK_EXTRA = 9,		//task_extra_info
		E_TBL_CAMERA = 10,			//camera_info
		E_TBL_FENCE = 11,				//fence_info
		E_TBL_TASK_FENCE = 12,	//fence_task_info
		E_TBL_DEVICE_CHARGE = 13, //device_charge_info
	};

	enum eEscortDBAlarmType
	{
		E_ALMTYPE_OFFLINE = 0,
		E_ALMTYPE_FLEE = 1,
		E_ALMTYPE_LOWPOWER = 2,
		E_ALMTYPE_LOOSE = 3,
		E_ALMTYPE_FENCE = 4,
		E_ALMTYPE_LOST_LOCATE = 5,
		E_ALMTYPE_PEER_OVERBOUNDARY = 6,
	};

	enum eEscortDBLocateType
	{
		E_LOCATE_GPS = 0,
		E_LOCATE_LBS = 1,
		E_LOCATE_WIFI = 2,
		E_LOCATE_APP = 3,
	};

	enum eEscortDBFenceType
	{
		E_FENCE_POLYGON = 0,
		E_FENCE_RECT = 1,
		E_FENCE_CIRCLE = 2,
	};

	typedef struct tagSqlContainer
	{
		unsigned long long ulSqlOptTime;
		unsigned int uiSqlOptSeq;
		unsigned short usSqlOptTarget;	//db-table
		unsigned short usSqlOptType: 8;		//dbOperate
		unsigned short usSqlKeyDesp : 8;
		char szSqlOptKey[64];
		unsigned int uiResultCount;
		unsigned int uiResultLen;
		unsigned char * pStoreResult;
	} SqlContainer;

	typedef struct tagSqlPerson
	{
		char szPersonId[32];
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
		int nUserRoleType; //1: admin, 2: app user, 3: admin & app user
		char szPhoneCode[16];
		tagSqlGuarder()
		{
			szUserId[0] = '\0';
			szUserName[0] = '\0';
			szPasswd[0] = '\0';
			szOrgId[0] = '\0';
			nUserRoleType = 0;
			szPhoneCode[0] = '\0';
		}
	} SqlGuarder;

	typedef struct tagSqlTask
	{
		char szTaskId[20];
		unsigned short usTaskType;
		unsigned short usTaskLimit;
		char szDestination[64];
		char szStartTime[20];
		char szGuarderId[20];
		char szDeviceId[16];
		char szHandset[64];
		char szTarget[64];
		char szPhone[32];
		int nFleeFlag;
		tagSqlTask()
		{
			szTaskId[0] = '\0';
			szDestination[0] = '\0';
			szStartTime[0] = '\0';
			szGuarderId[0] = '\0';
			szDeviceId[0] = '\0';
			szHandset[0] = '\0';
			szTarget[0] = '\0';
			szPhone[0] = '\0';
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
		int nLocationType : 16;
		int nCoordinate : 16;
		unsigned short usIsUse;
		unsigned short usBattery;
		unsigned short usOnline;
		unsigned short usIsRemove;
		int nMnc : 16;
		int nCharge : 16;
		char szImei[20];
		tagSqlDevice()
		{
			szDeviceId[0] = '\0';
			szFactoryId[0] = '\0';
			szOrgId[0] = '\0';
			szLastCommuncation[0] = '\0';
			szLastLocation[0] = '\0';
			dLat = dLng = 0.000000;
			usIsRemove = 0;
			nMnc = 0;
			nCharge = 0;
			nCoordinate = 0;
			szImei[0] = '\0';
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
		char szMsgBody[512];
		char szMsgFrom[40];
		unsigned long long ulMsgTime;
		tagSqlMessage()
		{
			szMsgTopic[0] = '\0';
			szMsgUuid[0] = '\0';
			szMsgBody[0] = '\0';
			szMsgFrom[0] = '\0';
			usMsgType = 0;
			usMsgSeq = 0;
			ulMsgTime = 0;
		}
	} SqlMessage;
	
	typedef struct tagSqlFence
	{
		int nFenceId; 
		unsigned short usFenceType;		//0-polygon;1-rect;2-circle; 
		uint8_t nFenceActive; //0-deactive;1-active
		uint8_t nCoordinate; //0:standard;1:
		char szFenceContent[512];
		tagSqlFence()
		{
			nFenceId = 0;
			usFenceType = 0;
			nFenceActive = 0;
			nCoordinate = 0;
			szFenceContent[0] = '\0';
		}
	} SqlFence;

	typedef struct tagSqlFenceTask
	{
		int nFenceTaskId;
		int nFenceId;
		int nTaskState: 16;//0-unfinish;1-finish;2-ignore
		int nFencePolicy: 8;//0:in fence alram;1:out fence alarm
		int nPeerCheck : 8;
		char szFactoryId[4];
		char szDeviceId[16];
		char szTaskStartTime[20];
		char szTaskStopTime[20];
		tagSqlFenceTask()
		{
			nFenceTaskId = 0;
			nFenceId = 0;
			nTaskState = 0;
			nFencePolicy = 0;
			szFactoryId[0] = '\0';
			szDeviceId[0] = '\0';
			szTaskStartTime[0] = '\0';
			szTaskStopTime[0] = '\0';
		}
	} SqlFenceTask;



}




#endif
