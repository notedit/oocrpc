// Date: 2012-06-13
// Author: ngaut <ngaut@126.com>
// simple cpp client for oocrpc

#pragma warning(disable:4996)

#define WIN32_LEAN_AND_MEAN
#include "../src/bson.h"
#include <iostream>
#include <vector>
#include <string>
#include <boost/asio.hpp>
#include <conio.h>

#pragma comment(lib, "libbson-cpp")

using namespace std;
using namespace bson;
using boost::asio::ip::tcp;


bool WriteRpcRequestHeader(tcp::socket& socket, const char* method ) 
{
	boost::system::error_code ec;
	bob b;

	b.append("operation", int(1));
	b.append("method", method);

	BSONObj o = b.obj();
	int len = o.objsize();

	boost::asio::write(socket, boost::asio::buffer(o.objdata(), len), ec);
	if (ec)
		return false;

	return true;
}

bool WriteRpcRequestArgs(tcp::socket& socket,  BSONObj& arg ) 
{
	boost::system::error_code ec;
	bob b;

	for( bo::iterator i(arg); i.more(); ) {
		b.append(i.next());
	}

	BSONObj o = b.obj();
	int len = o.objsize();

	boost::asio::write(socket, boost::asio::buffer(o.objdata(), len), ec);
	if (ec)
		return false;

	return true;
}


int ReadRpcResponseHeader(tcp::socket& socket) 
{
	boost::system::error_code ec;
	unsigned int responseLen;
	boost::asio::read(socket, boost::asio::buffer(&responseLen, sizeof(responseLen)), ec);
	if (ec){
		std::cout << ec << std::endl;
		return -1;
	}

	char* leftData = (char* )malloc(responseLen);
	*(uint32_t*)leftData = responseLen;

	boost::asio::read(socket, boost::asio::buffer(leftData + sizeof(uint32_t), responseLen - sizeof(int)), ec);
	if (ec){
		std::cout << ec << std::endl;
		return -1;
	}
	else
	{
		BSONObj o(leftData);
		
		if (o.getField("error").str().size() > 0)	//error found!!
		{
			//cout << o.jsonString(JS, 1) << endl;
			free(leftData);
			return 0;
		}
	}

	free(leftData);

	return 1;
}

bool ReadRpcResponseBody(tcp::socket& socket, BSONObj* result) 
{
	boost::system::error_code ec;
	unsigned int responseLen;
	boost::asio::read(socket, boost::asio::buffer(&responseLen, sizeof(responseLen)), ec);
	if (ec){
		std::cout << ec << std::endl;
	}

	char* leftData = (char* )malloc(responseLen);
	*(uint32_t*)leftData = responseLen;
	boost::asio::read(socket, boost::asio::buffer(leftData + sizeof(uint32_t), responseLen - sizeof(int)), ec);
	if (ec){
		free(leftData);
		std::cout << ec << std::endl;
		return false;
	}
	else
	{
		*result = BSONObj(leftData).copy();
	}

	free(leftData);

	return true;
}

bool DoRpcCall(const char* host, const char* method, BSONObj& arg, BSONObj* result)
{
	boost::asio::io_service io_service;

	tcp::socket s(io_service);
	boost::system::error_code ec;
	s.connect(  boost::asio::ip::tcp::endpoint(boost::asio::ip::address::from_string(host), 9091), ec); 
	if (ec)
	{
		cout << ec.message() << endl;
		return false;
	}

	boost::asio::ip::tcp::no_delay option(true);
	s.set_option(option, ec);
	if (ec)
	{
		cout << ec.message() << endl;
		return false;
	}

	if ( !WriteRpcRequestHeader(s, method) )
	{
		s.close();
		return false;
	}

	//write request body
	if ( !WriteRpcRequestArgs(s, arg) )
	{
		s.close();
		return false;
	}

	int rpcResult = ReadRpcResponseHeader(s);
	if ( rpcResult == -1 )	//network error
	{
		s.close();
		return false;
	}

	if ( !ReadRpcResponseBody(s, result) )
	{
		s.close();
		return false;
	}

	s.shutdown(boost::asio::socket_base::shutdown_both);
	s.close();
	
	if (rpcResult == 0)	//rpc response body is an empty bson object
	{
		return false;
	}
	
	return true;
}

void TestArithAdd(const char* host) 
{
	bob b;
	b.append("a", int(7));
	b.append("b", int(8));

	BSONObj result;
	int c = 0;
	if ( DoRpcCall(host, "Arith.Add", b.obj(), &result) )	//
	{
		//cout << result.jsonString(JS, 1) << endl;
		c = result["c"].Int();
		assert(c == 15);
	}
	else
	{
		cout << result.jsonString(JS, 1) << endl;
		assert(false);
	}
}

void NError(const char* host) 
{
	bob b;
	b.append("a", int(7));
	b.append("b", int(8));

	BSONObj result;
	int c = 0;
	if ( DoRpcCall(host, "Arith.NError", b.obj(), &result) )	//Arith.Add
	{
		cout << "never happened" << endl;
		cout << result.jsonString(JS, 1) << endl;
		assert(false);
	}
}

void DoTest(const char* host) 
{
	while (true)
	{
		time_t start  = time(NULL);
		int count = 10000;

		for (int i = 0; i < count; i++)
		{
			NError(host);
			TestArithAdd(host);
		}

		time_t end = time(NULL);
		printf("rpc speed %d\n", count * 2 / (end - start));
	}
}


int main(int argc, char** argv) {
	try
	{
		if (argc != 2)
		{
			std::cerr << "Usage: client <host>" << std::endl;
			return 1;
		}

		DoTest(argv[1]);

		getch();

	}
	catch (std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}

    return 0;
}

