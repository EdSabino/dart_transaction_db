import 'dart:io';

import 'package:csv/csv.dart';
import 'package:postgres/postgres.dart';

void main() {
  makeAll();
}

Future<void> makeAll() async {
  await execute("Tempo de execução transação: ", start);
  await execute("Tempo de execução transação implicita: ", startImplicit);
  // await execute("Tempo de execução transação com erro: ", startExplicitError); -> Isso da rollback e da erro
  // await execute("Tempo de execução transação implicita com erro: ", startImplicitError);  -> Isso da erro
}

Future<void> execute(String message, Future<void> Function(PostgreSQLConnection) func) async {
  PostgreSQLConnection conn = PostgreSQLConnection("localhost", 5432, "postgres", username: "postgres", password: "postgres");
  await conn.open();
  var startTime = DateTime.now();
  try {
    await func(conn);
  } catch (e) {
  }
  print(message + "${DateTime.now().difference(startTime)}");
  conn.query("DELETE FROM product");
}

Future<void> start(PostgreSQLConnection connection) async {
  List<List<dynamic>> items = await readFile();
  await connection.transaction((ctx) async {
    items.asMap().forEach((i, element) => insert(i, element, ctx));
  });
}

Future<void> startImplicit(PostgreSQLConnection connection) async {
  List<List<dynamic>> items = await readFile();
  await Future.forEach(items.asMap().keys, (i) async {
    await insert(i, items[i], connection);
  });
}

Future<void> startExplicitError(PostgreSQLConnection connection) async {
  List<List<dynamic>> items = await readFile();
  await connection.transaction((ctx) async {
    items.asMap().forEach((i, element) async {
      try {
        await insert(i, element, ctx);
        if (i == 12) {
          ctx.cancelTransaction();
          return;
        }
      } catch (e) {
        rethrow;
      } 
    });
  });
}

Future<void> startImplicitError(PostgreSQLConnection connection) async {
  List<List<dynamic>> items = await readFile();
  await Future.forEach(items.asMap().keys, (i) async {
    await insert(i, items[i], connection);
    if (i == 12) {
      throw Exception('stop') ;
    }
  });
}

Future<void> insert(int i, List<dynamic> item, PostgreSQLExecutionContext ctx) async {
  if (i == 0) {
    return;
  }
  await ctx.query("INSERT INTO product (eid, description) VALUES (@eid:int4, @description)", substitutionValues: {
    "eid" : i,
    "description": item[0]
  });
}

Future<List<List<dynamic>>> readFile() async {
  var file = await File('data.csv').readAsString();
  return CsvToListConverter().convert(file);
}