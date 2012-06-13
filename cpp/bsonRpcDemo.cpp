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

void iter(bo o) {
    /* iterator example */
    cout << "\niter()\n";
    for( bo::iterator i(o); i.more(); ) {
        cout << ' ' << i.next().toString() << '\n';
    }
}

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
	boost::asio::ip::tcp::no_delay option(true);
	s.set_option(option);

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
}

void NError(const char* host) 
{
	bob b;
	b.append("a", int(7));
	b.append("b", int(8));

	BSONObj result;
	int c = 0;
	if ( DoRpcCall(host, "NError", b.obj(), &result) )	//Arith.Add
	{
		//cout << result.jsonString(JS, 1) << endl;
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

#if 0
    cout << "build bits: " << 8 * sizeof(char *) << '\n' <<  endl;

    /* a bson object defaults on construction to { } */
    bo empty;
    cout << "empty: " << empty << endl;

    /* make a simple { name : 'joe', age : 33.7 } object */
    {
        bob b;
        b.append("name", "joe");
        b.append("age", 33.7);
        b.obj();
    }

    /* make { name : 'joe', age : 33.7 } with a more compact notation. */
    bo x = bob().append("name", "joe").append("age", 33.7).obj();

    /* convert from bson to json */
    string json = x.toString();
    cout << "json for x:" << json << endl;

    /* access some fields of bson object x */
    cout << "Some x things: " << x["name"] << ' ' << x["age"].Number() << ' ' << x.isEmpty() << endl;

    /* make a bit more complex object with some nesting
       { x : 'asdf', y : true, subobj : { z : 3, q : 4 } }
    */
    bo y = BSON( "x" << "asdf" << "y" << true << "subobj" << BSON( "z" << 3 << "q" << 4 ) );

    /* print it */
    cout << "y: " << y << endl;

    /* reach in and get subobj.z */
    cout << "subobj.z: " << y.getFieldDotted("subobj.z").Number() << endl;

    /* alternate syntax: */
    cout << "subobj.z: " << y["subobj"]["z"].Number() << endl;

    /* fetch all *top level* elements from object y into a vector */
    vector<be> v;
    y.elems(v);
    cout << v[0] << endl;

    /* into an array */
    list<be> L;
    y.elems(L);

    bo sub = y["subobj"].Obj();

    /* grab all the int's that were in subobj.  if it had elements that were not ints, we throw an exception
       (capital V on Vals() means exception if wrong type found
    */
    vector<int> myints;
    sub.Vals(myints);
    cout << "my ints: " << myints[0] << ' ' << myints[1] << endl;

    /* grab all the string values from x.  if the field isn't of string type, just skip it --
       lowercase v on vals() indicates skip don't throw.
    */
    vector<string> strs;
    x.vals(strs);
    cout << strs.size() << " strings, first one: " << strs[0] << endl;

    iter(y);
#endif
    return 0;
}

