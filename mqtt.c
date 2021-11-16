#include "stdio.h"
#include "string.h"


/* MQTTString */
typedef struct
{
	int len;
	char* data;
} MQTTLenString;

typedef struct
{
	char* cstring;
	MQTTLenString lenstring;
} MQTTString;

/* MQTTPacket_willOptions */
typedef struct
{
	/** The eyecatcher for this structure.  must be MQTW. */
	char struct_id[4];
	/** The version number of this structure.  Must be 0 */
	int struct_version;
	/** The LWT topic to which the LWT message will be published. */
	MQTTString topicName;
	/** The LWT payload. */
	MQTTString message;
	/**
      * The retained flag for the LWT message (see MQTTAsync_message.retained).
      */
	unsigned char retained;
	/**
      * The quality of service setting for the LWT message (see
      * MQTTAsync_message.qos and @ref qos).
      */
	char qos;
} MQTTPacket_willOptions;


/* MQTTPacket_connectData */
typedef struct
{
	/** The eyecatcher for this structure.  must be MQTC. */
	char struct_id[4];
	/** The version number of this structure.  Must be 0 */
	int struct_version;
	/** Version of MQTT to be used.  3 = 3.1 4 = 3.1.1
	  */
	unsigned char MQTTVersion;
	MQTTString clientID;
	unsigned short keepAliveInterval;
	unsigned char cleansession;
	unsigned char willFlag;
	MQTTPacket_willOptions will;
	MQTTString username;
	MQTTString password;
} MQTTPacket_connectData;


/* MQTTHeader */
typedef union
{
	unsigned char byte;	                /**< the whole byte */
#if defined(REVERSED)
	struct
	{
		unsigned int type : 4;			/**< message type nibble */
		unsigned int dup : 1;				/**< DUP flag bit */
		unsigned int qos : 2;				/**< QoS value, 0, 1 or 2 */
		unsigned int retain : 1;		/**< retained flag bit */
	} bits;
#else
	struct
	{
		unsigned int retain : 1;		/**< retained flag bit */
		unsigned int qos : 2;				/**< QoS value, 0, 1 or 2 */
		unsigned int dup : 1;				/**< DUP flag bit */
		unsigned int type : 4;			/**< message type nibble */
	} bits;
#endif
} MQTTHeader;


/* MQTTConnectFlags */
typedef union
{
	unsigned char all;	/**< all connect flags */
#if defined(REVERSED)
	struct
	{
		unsigned int username : 1;			/**< 3.1 user name */
		unsigned int password : 1; 			/**< 3.1 password */
		unsigned int willRetain : 1;		/**< will retain setting */
		unsigned int willQoS : 2;				/**< will QoS value */
		unsigned int will : 1;			    /**< will flag */
		unsigned int cleansession : 1;	  /**< clean session flag */
		unsigned int : 1;	  	          /**< unused */
	} bits;
#else
	struct
	{
		unsigned int : 1;	     					/**< unused */
		unsigned int cleansession : 1;	  /**< cleansession flag */
		unsigned int will : 1;			    /**< will flag */
		unsigned int willQoS : 2;				/**< will QoS value */
		unsigned int willRetain : 1;		/**< will retain setting */
		unsigned int password : 1; 			/**< 3.1 password */
		unsigned int username : 1;			/**< 3.1 user name */
	} bits;
#endif
} MQTTConnectFlags;	/**< connect flags byte */


/* writeChar */
void writeChar(unsigned char** pptr, char c)
{
	**pptr = c;
	(*pptr)++;
}


/* MQTTSerialize_connectLength */
#define FUNC_ENTRY
#define FUNC_ENTRY_NOLOG
#define FUNC_ENTRY_MED
#define FUNC_ENTRY_MAX
#define FUNC_EXIT
#define FUNC_EXIT_NOLOG
#define FUNC_EXIT_MED
#define FUNC_EXIT_MAX
#define FUNC_EXIT_RC(x)
#define FUNC_EXIT_MED_RC(x)
#define FUNC_EXIT_MAX_RC(x)

int MQTTstrlen(MQTTString mqttstring)
{
	int rc = 0;

	if (mqttstring.cstring)
		rc = strlen(mqttstring.cstring);
	else
		rc = mqttstring.lenstring.len;
	return rc;
}

int MQTTSerialize_connectLength(MQTTPacket_connectData* options)
{
	int len = 0;

	FUNC_ENTRY;

	if (options->MQTTVersion == 3)
		len = 12; /* variable depending on MQTT or MQIsdp */
	else if (options->MQTTVersion == 4)
		len = 10;

	len += MQTTstrlen(options->clientID)+2;
	if (options->willFlag)
		len += MQTTstrlen(options->will.topicName)+2 + MQTTstrlen(options->will.message)+2;
	if (options->username.cstring || options->username.lenstring.data)
		len += MQTTstrlen(options->username)+2;
	if (options->password.cstring || options->password.lenstring.data)
		len += MQTTstrlen(options->password)+2;

	FUNC_EXIT_RC(len);
	return len;
}


/* MQTTPacket_len */
int MQTTPacket_len(int rem_len)
{
	rem_len += 1; /* header byte */

	/* now remaining_length field */
	if (rem_len < 128)
		rem_len += 1;
	else if (rem_len < 16384)
		rem_len += 2;
	else if (rem_len < 2097151)
		rem_len += 3;
	else
		rem_len += 4;
	return rem_len;
}


enum errors
{
	MQTTPACKET_BUFFER_TOO_SHORT = -2,
	MQTTPACKET_READ_ERROR = -1,
	MQTTPACKET_READ_COMPLETE
};

enum msgTypes
{
	CONNECT = 1, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL,
	PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK,
	PINGREQ, PINGRESP, DISCONNECT
};

/* MQTTPacket_encode */
int MQTTPacket_encode(unsigned char* buf, int length)
{
	int rc = 0;

	FUNC_ENTRY;
	do
	{
		char d = length % 128;
		length /= 128;
		/* if there are more digits to encode, set the top bit of this digit */
		if (length > 0)
			d |= 0x80;
		buf[rc++] = d;
	} while (length > 0);
	FUNC_EXIT_RC(rc);
	return rc;
}


void writeInt(unsigned char** pptr, int anInt)
{
	**pptr = (unsigned char)(anInt / 256);
	(*pptr)++;
	**pptr = (unsigned char)(anInt % 256);
	(*pptr)++;
}


void writeCString(unsigned char** pptr, const char* string)
{
	int len = strlen(string);
	writeInt(pptr, len);
	memcpy(*pptr, string, len);
	*pptr += len;
}


void writeMQTTString(unsigned char** pptr, MQTTString mqttstring)
{
	if (mqttstring.lenstring.len > 0)
	{
		writeInt(pptr, mqttstring.lenstring.len);
		memcpy(*pptr, mqttstring.lenstring.data, mqttstring.lenstring.len);
		*pptr += mqttstring.lenstring.len;
	}
	else if (mqttstring.cstring)
		writeCString(pptr, mqttstring.cstring);
	else
		writeInt(pptr, 0);
}


int MQTTSerialize_connect(unsigned char* buf, int buflen, MQTTPacket_connectData* options)
{
	unsigned char *ptr = buf;
	MQTTHeader header = {0};
	MQTTConnectFlags flags = {0};
	int len = 0;
	int rc = -1;

	FUNC_ENTRY;
	if (MQTTPacket_len(len = MQTTSerialize_connectLength(options)) > buflen)
	{
		rc = MQTTPACKET_BUFFER_TOO_SHORT;
		goto exit;
	}
	header.byte = 0;
	header.bits.type = CONNECT;
	writeChar(&ptr, header.byte); /* write header */	
	ptr += MQTTPacket_encode(ptr, len); /* write remaining length */

	if (options->MQTTVersion == 4)
	{
		writeCString(&ptr, "MQTT");
		writeChar(&ptr, (char) 4);
	}
	else    /* 版本是3 */
	{
		writeCString(&ptr, "MQIsdp");
		writeChar(&ptr, (char) 3);
	}	

	flags.all = 0;
	flags.bits.cleansession = options->cleansession;
	flags.bits.will = (options->willFlag) ? 1 : 0;

	if (flags.bits.will)
	{
		flags.bits.willQoS = options->will.qos;
		flags.bits.willRetain = options->will.retained;
	}

	if (options->username.cstring || options->username.lenstring.data)
		flags.bits.username = 1;
	if (options->password.cstring || options->password.lenstring.data)
		flags.bits.password = 1;

	writeChar(&ptr, flags.all);
	writeInt(&ptr, options->keepAliveInterval);
	writeMQTTString(&ptr, options->clientID);
	if (options->willFlag)
	{
		writeMQTTString(&ptr, options->will.topicName);
		writeMQTTString(&ptr, options->will.message);
	}
	if (flags.bits.username)
		writeMQTTString(&ptr, options->username);
	if (flags.bits.password)
		writeMQTTString(&ptr, options->password);

	rc = ptr - buf;

	exit: FUNC_EXIT_RC(rc);
	return rc;
}



typedef struct MQTTMessage
{
    enum QoS qos;
    unsigned char retained;
    unsigned char dup;
    unsigned short id;
    void *payload;
    size_t payloadlen;
} MQTTMessage;

typedef struct MessageData
{
    MQTTMessage* message;
    MQTTString* topicName;
} MessageData;


typedef struct Network Network;//网络接口 对外
struct Network
{
  int my_socket;//当前连接的TCP socket号
  //参数要求(c->ipstack, &i, 1, timeout) 返回读到的数据长度
  int (*mqttread) (Network* ipstack, unsigned char* R_buffer, int R_buffer_Lenth, int timeoutMS);//对服务器读 函数需要自己实现
  int (*mqttwrite) (Network*, unsigned char*, int, int);//对服务器写 参数同上 返回写入数据长度
  void (*disconnect) (Network);//断开TCP连接
};

typedef struct Timer Timer;//定时器接口 对外
struct Timer {
unsigned int StartVal;//定时初值
unsigned int end_time;//定时时间
};


#define MAX_MESSAGE_HANDLERS 5 /* redefinable - how many subscriptions do you want? */

typedef struct MQTTClient
{
    unsigned int next_packetid,
      command_timeout_ms;
    size_t buf_size,
      readbuf_size;
    unsigned char *buf,
      *readbuf;
    unsigned int keepAliveInterval;
    char ping_outstanding;
    int isconnected;
    int cleansession;

    struct MessageHandlers
    {
        const char* topicFilter;
        void (*fp) (MessageData*);
    } messageHandlers[MAX_MESSAGE_HANDLERS];      /* Message handlers are indexed by subscription topic */

    void (*defaultMessageHandler) (MessageData*);

    Network* ipstack;
    Timer last_sent, last_received;

} MQTTClient;


void MQTTClientInit(MQTTClient* c, Network* network, unsigned int command_timeout_ms,
		unsigned char* sendbuf, size_t sendbuf_size, unsigned char* readbuf, size_t readbuf_size)
{
    int i;
    c->ipstack = network;

    for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
        c->messageHandlers[i].topicFilter = 0;
    c->command_timeout_ms = command_timeout_ms;
    c->buf = sendbuf;
    c->buf_size = sendbuf_size;
    c->readbuf = readbuf;
    c->readbuf_size = readbuf_size;
    c->isconnected = 0;
    c->cleansession = 0;
    c->ping_outstanding = 0;
    c->defaultMessageHandler = NULL;
	c->next_packetid = 1;


}

#define MQTTPacket_willOptions_initializer { {'M', 'Q', 'T', 'W'}, 0, {NULL, {0, NULL}}, {NULL, {0, NULL}}, 0, 0 }

#define MQTTPacket_connectData_initializer { {'M', 'Q', 'T', 'C'}, 0, 4, {NULL, {0, NULL}}, 60, 1, 0, \
		MQTTPacket_willOptions_initializer, {NULL, {0, NULL}}, {NULL, {0, NULL}} }


int main()
{
	MQTTClient c;
	int i;
	unsigned char buf[1000];
	unsigned char readbuf[1000];
	char clientId[150] = {'6', '8', '0', '0',  '0',  '0',  '1',  '0',  '0',  '0',  '7', '6', '8', '0', '1', '7'};
	Network n;
	
	MQTTPacket_connectData data = MQTTPacket_connectData_initializer;       
	data.willFlag = 0;
	data.MQTTVersion = 4;
	data.clientID.cstring = clientId;
	data.username.cstring = 0;
	data.password.cstring = 0;
	data.keepAliveInterval = 60;
	data.cleansession = 1;

	MQTTClientInit(&c, &n, 1000, buf, sizeof(buf), readbuf, sizeof(readbuf));
	MQTTSerialize_connect(c.buf, c.buf_size, &data);
	
	printf("长度:%d\r\n", c.buf_size);


	//printf("printf 测试\r\n");
	for (i = 0; i < 100; i ++)
	{
		printf("%d \r\n", c.buf[i]);

	}
	



	
}