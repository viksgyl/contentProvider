import json
from pprint import pprint
import sys
import datetime
today = datetime.date.today()
NEWLINE = "\n"

if (len(sys.argv) < 4):
  print 'Passed number of arguments not sufficient, Usage = providergen.py <package name> <schema file> <output>'
  exit(0)

package = sys.argv[1]
schema = sys.argv[2]
output = sys.argv[3]
print package, schema, output

def CreateDbProvider(output_file, data):
  output_file.write("package %s.contentprovider;" % package + NEWLINE)
  output_file.write("" + NEWLINE);
  output_file.write("import android.content.ContentProvider;" + NEWLINE)
  output_file.write("import android.content.ContentResolver;" + NEWLINE)
  output_file.write("import android.content.ContentValues;" + NEWLINE)
  output_file.write("import android.content.UriMatcher;" + NEWLINE)
  output_file.write("import android.database.Cursor;" + NEWLINE)
  output_file.write("import android.database.sqlite.SQLiteDatabase;" + NEWLINE)
  output_file.write("import android.database.sqlite.SQLiteQueryBuilder;" + NEWLINE)
  output_file.write("import android.net.Uri;" + NEWLINE)
  output_file.write("import android.text.TextUtils;" + NEWLINE)

  output_file.write("" + NEWLINE)
  output_file.write('''
/**
* Created by providergen.py on %s
*/
''' % today)
  for table in data["Tables"]:
    output_file.write('import %s.database.%sTable;' % (package, table["TableName"]) + NEWLINE)
  output_file.write('import %sdatabase.%sDbHelper;' %(package, data["Database"]) + NEWLINE)
  output_file.write('import java.util.Arrays;' + NEWLINE)
  output_file.write('import java.util.HashSet;' + NEWLINE)
  output_file.write('public class %sContentProvider extends ContentProvider {' % Database + NEWLINE)
  output_file.write('  // database' + NEWLINE)
  output_file.write('  private %sDbHelper database;' % Database + NEWLINE)
  output_file.write('  // used for the UriMatcher' + NEWLINE)
  id = 100
  for table in data["Tables"]:
    output_file.write('  private static final int %s = %d;' % (table["TableName"], id) + NEWLINE)
    output_file.write('  private static final int %s_Id = %d;' % (table["TableName"], id + 10) + NEWLINE)
    id += 100
  output_file.write('  private static final String AUTHORITY = "%s.contentprovider";' % package + NEWLINE)
  output_file.write('  private static final String BASE_PATH = "%s";' % Database + NEWLINE)
  for table in data["Tables"]:
    tableName = table["TableName"]
    output_file.write('  public static final Uri %s_%s_Uri = Uri.parse("content://" + AUTHORITY' %(Database, tableName) + NEWLINE)
    output_file.write('          + "/" + BASE_PATH + "." + %sTable.Table_%s);' % (tableName, tableName) + NEWLINE)
    output_file.write('  public static final String %ss_Type = ContentResolver.CURSOR_DIR_BASE_TYPE + "/%ss";' % (tableName, tableName) + NEWLINE)
    output_file.write('  public static final String %s_Item_Type = ContentResolver.CURSOR_ITEM_BASE_TYPE + "/%s";' % (tableName, tableName) + NEWLINE)
    output_file.write('  private static final UriMatcher sURIMatcher = new UriMatcher(UriMatcher.NO_MATCH);' + NEWLINE)
    output_file.write('  static {' + NEWLINE)
    for table in data["Tables"]:
      tableName = table["TableName"]
      output_file.write('    sURIMatcher.addURI(AUTHORITY, BASE_PATH, %s);' % tableName + NEWLINE)
      output_file.write('    sURIMatcher.addURI(AUTHORITY, BASE_PATH + "/#", %s_Id);' % tableName + NEWLINE)
    output_file.write('  }' + NEWLINE)
    output_file.write('  @Override' + NEWLINE)
    output_file.write('  public boolean onCreate() {' + NEWLINE)
    output_file.write('    database = new %sDbHelper(getContext());' % Database + NEWLINE)
    output_file.write('    return false;' + NEWLINE)
    output_file.write('  }' + NEWLINE)
    output_file.write('  @Override' + NEWLINE)
    output_file.write('  public Cursor query(Uri uri, String[] projection, String selection,' + NEWLINE)
    output_file.write('    String[] selectionArgs, String sortOrder) {' + NEWLINE)
    output_file.write('     SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();' + NEWLINE)
    output_file.write('     checkColumns(uri, projection);' + NEWLINE + NEWLINE)
    output_file.write('     // Set the table' + NEWLINE)
    output_file.write('     int uriType = sURIMatcher.match(uri);' + NEWLINE)
    output_file.write('     switch (uriType) {' + NEWLINE)
    for table in data["Tables"]:
      tableName = table["TableName"]
      output_file.write('       case %s:' % tableName + NEWLINE)
      output_file.write('         queryBuilder.setTables(%sTable.Table_%s);' % (tableName, tableName) + NEWLINE)
      output_file.write('         break;' + NEWLINE)
      output_file.write('       case %s_Id:' % tableName + NEWLINE)
      output_file.write('         queryBuilder.setTables(%sTable.Table_%s);' % (tableName, tableName) + NEWLINE)
      output_file.write('         queryBuilder.appendWhere(%sTable.Col_Id + "=" + uri.getLastPathSegment() );' % tableName + NEWLINE)
      output_file.write('         break;' + NEWLINE)
    output_file.write('       default:' + NEWLINE)
    output_file.write('         throw new IllegalArgumentException("Unknown URI: " + uri);' + NEWLINE)
    output_file.write('     }' + NEWLINE)
    output_file.write('     SQLiteDatabase db = database.getWritableDatabase();' + NEWLINE)
    output_file.write('     Cursor cursor = queryBuilder.query(db, projection, selection,' + NEWLINE)
    output_file.write('       selectionArgs, null, null, sortOrder);' + NEWLINE)
    output_file.write('     // make sure that potential listeners are getting notified' + NEWLINE)
    output_file.write('     cursor.setNotificationUri(getContext().getContentResolver(), uri);' + NEWLINE)
    output_file.write('     return cursor;' + NEWLINE)
    output_file.write('  }' + NEWLINE + NEWLINE)
    output_file.write('  @Override' + NEWLINE)
    output_file.write('  public String getType(Uri uri) {' + NEWLINE)
    output_file.write('     return null;' + NEWLINE)
    output_file.write('  }' + NEWLINE + NEWLINE)
    output_file.write('  @Override' + NEWLINE)
    output_file.write('  public Uri insert(Uri uri, ContentValues values) {' + NEWLINE)
    output_file.write('     int uriType = sURIMatcher.match(uri);' + NEWLINE)
    output_file.write('     SQLiteDatabase sqlDB = database.getWritableDatabase();' + NEWLINE)
    output_file.write('     long id = 0;' + NEWLINE)
    output_file.write('     String path = AUTHORITY + "." + BASE_PATH;' + NEWLINE)
    output_file.write('     switch (uriType) {' + NEWLINE)
    for table in data["Tables"]:
      tableName = table["TableName"]
      output_file.write('       case %s:' % tableName + NEWLINE)
      output_file.write('         id = sqlDB.insert(%sTable.Table_%s, null, values);' % (tableName, tableName) + NEWLINE)
      output_file.write('         path = path + "." + %sTable.Table_%s;' %  (tableName, tableName) + NEWLINE)
      output_file.write('         break;' + NEWLINE)
    output_file.write('       default:' + NEWLINE)
    output_file.write('         throw new IllegalArgumentException("Unknown URI: " + uri);' + NEWLINE)
    output_file.write('     }' + NEWLINE)
    output_file.write('     getContext().getContentResolver().notifyChange(uri, null);' + NEWLINE)
    output_file.write('     return Uri.parse(path + "/" + id);' + NEWLINE)
    output_file.write('  }' + NEWLINE + NEWLINE)

    output_file.write('  @Override' + NEWLINE)
    output_file.write('  public int delete(Uri uri, String selection, String[] selectionArgs) {' + NEWLINE)
    output_file.write('     int uriType = sURIMatcher.match(uri);' + NEWLINE)
    output_file.write('     SQLiteDatabase sqlDB = database.getWritableDatabase();' + NEWLINE)
    output_file.write('     int rowsDeleted = 0;' + NEWLINE)
    output_file.write('     switch (uriType) {' + NEWLINE)
    for table in data["Tables"]:
      tableName = table["TableName"]
      output_file.write('       case %s:' % tableName + NEWLINE)
      output_file.write('         rowsDeleted = sqlDB.delete(%sTable.Table_%s, selection, selectionArgs);' % (tableName, tableName) + NEWLINE)
      output_file.write('         break;' + NEWLINE)
      output_file.write('       case %s_Id:' % tableName + NEWLINE)
      output_file.write('         String id = uri.getLastPathSegment();' + NEWLINE)
      output_file.write('         if (TextUtils.isEmpty(selection)) {' + NEWLINE)
      output_file.write('           rowsDeleted = sqlDB.delete(%sTable.Table_%s,' %  (tableName, tableName) + NEWLINE)
      output_file.write('             %sTable.Col_Id + "=" + id, null);' %  (tableName) + NEWLINE)
      output_file.write('         } else {' + NEWLINE)
      output_file.write('           rowsDeleted = sqlDB.delete(%s.Table_%s,' %  (tableName, tableName) + NEWLINE)
      output_file.write('             %sTable.Col_Id + "=" + id' %  (tableName) + NEWLINE)
      output_file.write('             + " and " + selection, selectionArgs);'  + NEWLINE)
      output_file.write('         }' + NEWLINE)
      output_file.write('         break;' + NEWLINE)
    output_file.write('       default:' + NEWLINE)
    output_file.write('         throw new IllegalArgumentException("Unknown URI: " + uri);' + NEWLINE)
    output_file.write('     }' + NEWLINE)
    output_file.write('     getContext().getContentResolver().notifyChange(uri, null);' + NEWLINE)
    output_file.write('     return rowsDeleted' + NEWLINE)
    output_file.write('  }' + NEWLINE + NEWLINE)

    output_file.write('  @Override' + NEWLINE)
    output_file.write('  public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {' + NEWLINE)
    output_file.write('     int uriType = sURIMatcher.match(uri);' + NEWLINE)
    output_file.write('     SQLiteDatabase sqlDB = database.getWritableDatabase();' + NEWLINE)
    output_file.write('     int rowsUpdated = 0;' + NEWLINE)
    output_file.write('     switch (uriType) {' + NEWLINE)
    for table in data["Tables"]:
      tableName = table["TableName"]
      output_file.write('       case %s:' % tableName + NEWLINE)
      output_file.write('         rowsUpdated = sqlDB.update(%sTable.Table_%s, values, selection, selectionArgs);' % (tableName, tableName) + NEWLINE)
      output_file.write('         break;' + NEWLINE)
      output_file.write('       case %s_Id:' % tableName + NEWLINE)
      output_file.write('         String id = uri.getLastPathSegment();' + NEWLINE)
      output_file.write('         if (TextUtils.isEmpty(selection)) {' + NEWLINE)
      output_file.write('           rowsDeleted = sqlDB.update(%sTable.Table_%s, values, ' %  (tableName, tableName) + NEWLINE)
      output_file.write('             %sTable.Col_Id + "=" + id, null);' %  (tableName) + NEWLINE)
      output_file.write('         } else {' + NEWLINE)
      output_file.write('           rowsUpdated = sqlDB.update(%sTable.Table_%s, values,' %  (tableName, tableName) + NEWLINE)
      output_file.write('             %sTable.Col_Id + "=" + id' %  (tableName) + NEWLINE)
      output_file.write('             + " and " + selection, selectionArgs);'  + NEWLINE)
      output_file.write('         }' + NEWLINE)
      output_file.write('         break;' + NEWLINE)
    output_file.write('       default:' + NEWLINE)
    output_file.write('         throw new IllegalArgumentException("Unknown URI: " + uri);' + NEWLINE)
    output_file.write('     }' + NEWLINE)
    output_file.write('     getContext().getContentResolver().notifyChange(uri, null);' + NEWLINE)
    output_file.write('     return rowsUpdated' + NEWLINE)
    output_file.write('  }' + NEWLINE + NEWLINE)

    output_file.write('  private void checkColumns(Uri uri, String[] projection) {' + NEWLINE + NEWLINE)
    for table in data["Tables"]:
      tableName = table["TableName"]
      output_file.write('     String[]available%s = {' % tableName + NEWLINE)
      for field in table["Fields"]:
        output_file.write('       %sTable.Col_%s,' % (tableName, field["FieldName"]) + NEWLINE)
      output_file.write('     };' + NEWLINE)
      output_file.write('     if (projection != null) {' + NEWLINE)
      output_file.write('       HashSet<String> requestedColumns = new HashSet<String>(Arrays.asList(projection));' + NEWLINE)
      output_file.write('       HashSet<String> availableColumns;' + NEWLINE)
      output_file.write('       int uriType = sURIMatcher.match(uri);' + NEWLINE)
      output_file.write('       switch (uriType) {' + NEWLINE)
      for table in data["Tables"]:
        tableName = table["TableName"]
      output_file.write('         case %s:' % tableName + NEWLINE)
      output_file.write('         case %s_Id:' % tableName + NEWLINE)
      output_file.write('           availableColumns = new HashSet<String>(Arrays.asList(available%s));' % tableName + NEWLINE)
      output_file.write('           if (!availableColumns.containsAll(requestedColumns)) {' + NEWLINE)
      output_file.write('             throw new IllegalArgumentException("Unknown columns in projection");' + NEWLINE)
      output_file.write('           }' + NEWLINE)
      output_file.write('           break;' + NEWLINE)
    output_file.write('       default:' + NEWLINE)
    output_file.write('         throw new IllegalArgumentException("Unknown columns in projection");' + NEWLINE)
    output_file.write('     }' + NEWLINE)
    output_file.write('   }' + NEWLINE)
    output_file.write(' }' + NEWLINE)
    output_file.write('}' + NEWLINE)

def CreateDatabase(dbhelper_file, data):
  Database = data["Database"]
  dbhelper_file.write("package %s.database;" % package + NEWLINE)
  dbhelper_file.write("import android.content.Context;" + NEWLINE)
  dbhelper_file.write("import android.database.sqlite.SQLiteDatabase;" + NEWLINE)
  dbhelper_file.write("import android.database.sqlite.SQLiteOpenHelper;" + NEWLINE)
  dbhelper_file.write('''

/**
* Created by providergen.py on %s
*/

''' % today)
  dbhelper_file.write("public class %sDbHelper extends SQLiteOpenHelper {" %Database + NEWLINE)
  dbhelper_file.write('  private static final String Database_Name = "%s.db";' % Database.lower() + NEWLINE)
  dbhelper_file.write('  private static final int Database_version = 1;\n' + NEWLINE)
  dbhelper_file.write('  public %sDbHelper(Context context) {' % Database + NEWLINE)
  dbhelper_file.write('    super(context, Database_Name, null, Database_Version);' + NEWLINE)
  dbhelper_file.write('  }\n' + NEWLINE)
  dbhelper_file.write('  // Method is called during creation of the database' + NEWLINE);
  dbhelper_file.write('  @Override' + NEWLINE)
  dbhelper_file.write('  onCreate(SQLiteDatabase database) {' + NEWLINE)
  for table in data["Tables"]:
    dbhelper_file.write('    %sTable.onCreate(database);' % table["TableName"] + NEWLINE) 
  dbhelper_file.write('  }\n' + NEWLINE)
  dbhelper_file.write('  // Method is called during an upgrade of the database,' + NEWLINE)
  dbhelper_file.write('  // e.g. if you increase the database version' + NEWLINE)
  dbhelper_file.write('  @Override' + NEWLINE)
  dbhelper_file.write('  public void onUpgrade(SQLiteDatabase database, int oldVersion, int newVersion) {' + NEWLINE)
  for table in data["Tables"]:
    dbhelper_file.write('    %sTable.onUpgrade(database, oldVersion, newVersion);' % table["TableName"] + NEWLINE)
  dbhelper_file.write('  }\n' + NEWLINE)
  dbhelper_file.write("}" + NEWLINE);

def TableHeader(table_data):
  table_data.write("package %s.database;" % package + NEWLINE)
  table_data.write("import android.database.sqlite.SQLiteDatabase;" + NEWLINE);
  table_data.write("import android.util.Log;" + NEWLINE);
  table_data.write('''
/**
* Created by providergen.py on %s
*/
''' % today)

def CreateTable(table):
  tableName = table["TableName"]
  table_data = open(tableName + 'Table.java', "w")
  TableHeader(table_data)
  table_data.write(NEWLINE + "public class %sTable {\n\n   //Database Table" % tableName)
  const_stmt = NEWLINE + '   public static final String Col_%s = "%s";'
  table_data.write(NEWLINE + '   public static final String Table_%s = "%s";' % (tableName, tableName));
  table_data.write(const_stmt % ("id","id"))
  for field in table["Fields"]:
    table_data.write(const_stmt % (field["FieldName"], field["FieldName"]))

  table_data.write(NEWLINE + '   //Table creation SQL statement')
  stmt = NEWLINE + '   private static final String TABLE_Create = "CREATE TABLE "'
  stmt += NEWLINE + "    + Table_%s" % tableName
  stmt += NEWLINE + '    + "("' 
  stmt += NEWLINE + '    + Col_%s + " INTEGER PRIMARY KEY AUTOINCREMENT, "' % "id" + NEWLINE
  fields = ""
  for field in table["Fields"]:
    fields += ('    + Col_%s + " %s') % (field["FieldName"], field["Type"])
    if (("AllowNull" in field) and (field["AllowNull"] == "False")):
      fields += " NOT NULL"
    if (field != table["Fields"][-1]):
      fields += ', "' + NEWLINE
  table_data.write('%s%s)";' % (stmt, fields) + NEWLINE)
  table_data.write(NEWLINE + "   public static void onCreate(SQLiteDatabase database) {");
  table_data.write(NEWLINE + "     database.execSQL(Table_Create); ");
  table_data.write(NEWLINE + "   }");
  table_data.write(NEWLINE + "\n   public static void onUpgrade(SQLiteDatabase database, int oldVersion, int newVersion) {");
  table_data.write(NEWLINE + '     Log.w(%s.class.getName(), "upgrading table from version " + oldVersion ' % tableName)  
  table_data.write(NEWLINE + '     + "  to " + newVersion');
  table_data.write(NEWLINE + '     + "  , which will destroy all old data");');
  table_data.write(NEWLINE + '     database.execSQL("DROP TABLE IF EXIST " + Table_%s);' % tableName)
  table_data.write(NEWLINE + '     onCreate(Table_Create);');
  table_data.write(NEWLINE + '  }');
  table_data.write(NEWLINE + '}' + NEWLINE);
  table_data.close()

data=""
with open(schema) as data_file:
  data = json.load(data_file)
Tables = data["Tables"]
for table in Tables:
  CreateTable(table)
Database = data["Database"]
output_file= open(Database + "DbHelper.java", 'w')
CreateDatabase(output_file, data)
output_file= open(Database + "ContentProvider.java", 'w')
CreateDbProvider(output_file, data)
output_file.close()
