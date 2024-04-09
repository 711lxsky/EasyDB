package top.lxsky711.easydb.core.tbm;

import top.lxsky711.easydb.common.data.ByteParser;
import top.lxsky711.easydb.common.log.Log;
import top.lxsky711.easydb.common.log.WarningMessage;
import top.lxsky711.easydb.core.dm.DataManager;
import top.lxsky711.easydb.core.sp.SPSetting;
import top.lxsky711.easydb.core.vm.VersionManager;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author: 711lxsky
 * @Description:
 */

public class TableManagerImpl implements TableManager{

    private VersionManager vm;

    private DataManager dm;

    private Booter booter;

    private Map<String, Table> tableCache;

    private Map<Long, List<Table>> transactionTableCache;

    private Lock selfLock;

    public TableManagerImpl(VersionManager vm, DataManager dm, Booter booter){
        this.vm = vm;
        this.dm = dm;
        this.booter = booter;
        this.tableCache = new HashMap<>();
        this.transactionTableCache = new HashMap<>();
        this.selfLock = new ReentrantLock();
        this.loadTables();
    }

    private void loadTables(){
        long tableUid = this.getFirstTableUid();
        while(tableUid != TBMSetting.TABLE_UID_DEFAULT){
            Table table = Table.loadTable(this, tableUid);
            tableUid = table.getNextTableUid();
            tableCache.put(table.getTableName(), table);
        }
    }

    private long getFirstTableUid(){
        byte[] tableInfoBytes = this.booter.readAllBytesDataInBooterFile();
        return ByteParser.parseBytesToLong(tableInfoBytes);
    }

    @Override
    public TBMSetting.BeginResult begin(SPSetting.Begin begin) {
        TBMSetting.BeginResult result = new TBMSetting.BeginResult();
        result.transactionXid = this.vm.begin(begin.TRANSACTION_ISOLATION_LEVEL);
        result.result =ByteParser.parseStringToNormalBytes(SPSetting.TOKEN_BEGIN_DEFAULT);
        return result;
    }

    @Override
    public byte[] commit(long transactionXid) {
        this.vm.commit(transactionXid);
        return ByteParser.parseStringToNormalBytes(SPSetting.TOKEN_COMMIT_DEFAULT);
    }

    @Override
    public byte[] abort(long transactionXid) {
        this.vm.abort(transactionXid);
        return ByteParser.parseStringToNormalBytes(SPSetting.TOKEN_ABORT_DEFAULT);
    }

    private void updateFirstTableUid(long newFirstTableUid){
        byte[] uidBytes = ByteParser.longToBytes(newFirstTableUid);
        this.booter.updateBytesDataInBooterFile(uidBytes);
    }

    @Override
    public byte[] create(long transactionXid, SPSetting.Create create) {
        this.selfLock.lock();
        try {
            if(this.tableCache.containsKey(create.tableName)){
                Log.logWarningMessage(WarningMessage.DUPLICATE_CREATE_TABLE);
            }
            Table newTable = Table.createTable(transactionXid, this, this.getFirstTableUid(), create);
            this.updateFirstTableUid(newTable.getTableUid());
            this.tableCache.put(create.tableName, newTable);
            if(! this.transactionTableCache.containsKey(transactionXid)){
                this.transactionTableCache.put(transactionXid, new ArrayList<>());
            }
            this.transactionTableCache.get(transactionXid).add(newTable);
            return ByteParser.parseStringToNormalBytes(SPSetting.TOKEN_CREATE_DEFAULT + " " + create.tableName);
        }
        finally {
            this.selfLock.unlock();
        }
    }

    private Table getTableFromCache(String tableName){
        this.selfLock.lock();
        Table table = this.tableCache.get(tableName);
        this.selfLock.unlock();
        return table;
    }

    @Override
    public byte[] insert(long transactionXid, SPSetting.Insert insert) {
        Table tableFromCache = this.getTableFromCache(insert.tableName);
        if(Objects.isNull(tableFromCache)){
            Log.logWarningMessage(WarningMessage.TABLE_NOT_FOUND);
            return null;
        }
        tableFromCache.insert(transactionXid, insert);
        return ByteParser.parseStringToNormalBytes(SPSetting.TOKEN_INSERT_DEFAULT);
    }

    @Override
    public byte[] select(long transactionXid, SPSetting.Select select) {
        Table tableFromCache = this.getTableFromCache(select.tableName);
        if(Objects.isNull(tableFromCache)){
            Log.logWarningMessage(WarningMessage.TABLE_NOT_FOUND);
            return null;
        }
        return ByteParser.parseStringToNormalBytes(tableFromCache.select(transactionXid, select));
    }

    @Override
    public byte[] delete(long transactionXid, SPSetting.Delete delete) {
        Table tableFromCache = this.getTableFromCache(delete.tableName);
        if(Objects.isNull(tableFromCache)){
            Log.logWarningMessage(WarningMessage.TABLE_NOT_FOUND);
            return null;
        }
        int deleteNum = tableFromCache.delete(transactionXid, delete);
        return ByteParser.parseStringToNormalBytes(SPSetting.TOKEN_DELETE_DEFAULT + " " + deleteNum);
    }

    @Override
    public byte[] update(long transactionXid, SPSetting.Update update) {
        Table tableFromCache = this.getTableFromCache(update.tableName);
        if(Objects.isNull(tableFromCache)){
            Log.logWarningMessage(WarningMessage.TABLE_NOT_FOUND);
            return null;
        }
        int updateNum = tableFromCache.update(transactionXid, update);
        return ByteParser.parseStringToNormalBytes(SPSetting.TOKEN_UPDATE_DEFAULT + " " + updateNum);
    }

    @Override
    public byte[] show(long transactionXid) {
        this.selfLock.lock();
        try {
            Collection<Table> allTables = Stream.concat(
                    tableCache.values().stream(),
                    Optional.ofNullable(transactionTableCache.get(transactionXid)).orElseGet(Collections::emptyList).stream()
            ).collect(Collectors.toList());
            StringBuilder sb = new StringBuilder();
            for (Table tb : allTables) {
                sb.append(tb.toString()).append(TBMSetting.LINE_FEED);
            }
            return sb.toString().getBytes();
        }
        finally {
            this.selfLock.unlock();
        }
    }

    @Override
    public DataManager getDM() {
        return this.dm;
    }

    @Override
    public VersionManager getVM() {
        return this.vm;
    }
}