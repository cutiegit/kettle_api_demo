package cn.para.kettle_api_demo.etl;

import org.apache.commons.io.FileUtils;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.insertupdate.InsertUpdateMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;

import java.io.File;

/**
 * ktr文件：执行Kettle处理数据分为两步，
 * 1.配置数据处理文件规则，导入导出，数据清洗等。
 * 2.使用KettleEnvironment.init();初始化Kettle的运行环境。
 * 3.使用TransMeta转化配置。
 */
public class TransDemo2 {

    public static TransDemo2 transDemo2;

    // from-表名
    public static String from_tablename = "tabletest";
    // from-表主键，集合模式，可以多主键
    public static String[] from_column_key = {"AA"};
    // form-数据来源映射关系，按照顺序和to_column_group保持一致
    public static String[] from_columns_group = {"AA","BB"};
    // from-用于数据抽取sql的拼装
    public static String from_columns = "AA,BB";

    // to-表名
    public static String to_tablename = "tabletest";
    // to-表主键，顺序映射
    public static String[] to_column_key = {"AA"};
    // to-数据插入更新映射关系，和from_columns_group保持一致
    public static String[] to_columns_group = {"AA","BB"};

    /*// from-表名
    public static String from_tablename = "tbsys_log";
    // from-表主键，集合模式，可以多主键
    public static String[] from_column_key = {"LOGID"};
    // form-数据来源映射关系，按照顺序和to_column_group保持一致
    public static String[] from_columns_group = {"LOGID","LOGTYPE","OPCONTENT","OPACTION","LOGINMAC","LOGINIP","STATUS","REMARK","ORGID","OPTIME","OPID","OPNAME"};
    // from-用于数据抽取sql的拼装
    public static String from_columns = "LOGID,LOGTYPE,OPCONTENT,OPACTION,LOGINMAC,LOGINIP,STATUS,REMARK,ORGID,OPTIME,OPID,OPNAME";

    // to-表名
    public static String to_tablename = "tbsys_log";
    // to-表主键，顺序映射
    public static String[] to_column_key = {"LOGID"};
    // to-数据插入更新映射关系，和from_columns_group保持一致
    public static String[] to_columns_group = {"LOGID","LOGTYPE","OPCONTENT","OPACTION","LOGINMAC","LOGINIP","STATUS","REMARK","ORGID","OPTIME","OPID","OPNAME"};*/

    /**
     * 数据库连接信息,适用于DatabaseMeta其中 一个构造器DatabaseMeta(String xml)
     *
     * name: 连接名称
     * server：Ip地址
     * type：数据库类型
     * access：连接方式，Native指JDBC
     * database：base
     * port：端口
     * username：用户名
     * passeord：密码
     */
    public static final String[] databasesXML = {

            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<connection>" +
                    "<name>from</name>" +
                    "<server>localhost</server>" +
                    "<type>Oracle</type>" +
                    "<access>Native</access>" +
                    "<database>orcl</database>" +
                    "<port>1521</port>" +
                    "<username>sxfy2</username>" +
                    "<password>sxfy2</password>" +
                    "</connection>",
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
                    "<connection>" +
                    "<name>to</name>" +
                    "<server>localhost</server>" +
                    "<type>Generic database</type>" +
                    "<access>Native</access>" +
                    "<database>dm</database>" +
                    "<port>5236</port>" +
                    "<username>sysdba</username>" +
                    "<password>111111111</password>"
    };

    public static void main(String[] args) {
        try {
            long startTime = System.currentTimeMillis();

            KettleEnvironment.init();
            transDemo2 = new TransDemo2();
            TransMeta transMeta = transDemo2.generateMyOwnTrans();
            String transXml = transMeta.getXML();
            String transName = "etl/update_insert_Trans.ktr";
            File file = new File(transName);
            FileUtils.writeStringToFile(file, transXml, "UTF-8");
            System.out.println(databasesXML.length+"\n"+databasesXML[0]+"\n"+databasesXML[1]);
            //创建转换元数据对象
            TransMeta meta = new TransMeta("etl/update_insert_Trans.ktr");
//            TransMeta meta = new TransMeta("G://1.ktr");
            Trans trans = new Trans(meta);
            trans.prepareExecution(null);
            trans.startThreads();
            trans.waitUntilFinished();
            if(trans.getErrors()!=0){
                System.out.println("执行失败！");
            }

            long endTime = System.currentTimeMillis();
            System.out.println("程序总运行时间： " + (endTime - startTime)/1000 + "s");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
    /**
     * 数据清洗转化
     * @return
     * @throws KettleXMLException
     */
    public TransMeta oneTrans() throws KettleXMLException {
        System.out.println("************start to data wash get***********");
        TransMeta transMeta = new TransMeta();
        //设置转化的名称
        transMeta.setName("data_wash");
        //添加转换的数据库连接
//        DatabaseMeta databaseMeta = new DatabaseMeta(databasesXMLOne);
        DatabaseMeta databaseMeta = new DatabaseMeta();
        transMeta.addDatabase(databaseMeta);
        //添加一个DatabaseMeta连接数据库
        DatabaseMeta database = transMeta.findDatabase("wash");



        return null;
    }

    /**
     * 生成一个转化,把一个数据库中的数据转移到另一个数据库中,只有两个步骤,第一个是表输入,第二个是表插入与更新操作
     * @return
     * @throws KettleXMLException
     */
    public TransMeta generateMyOwnTrans() throws KettleXMLException, KettleDatabaseException {
        System.out.println("************start to generate my own transformation***********");
        TransMeta transMeta = new TransMeta();
        //设置转化的名称
        transMeta.setName("insert_update");
        //添加转换的数据库连接
        for (int i=0;i<databasesXML.length;i++){
            DatabaseMeta databaseMeta = new DatabaseMeta(databasesXML[0]);
            transMeta.addDatabase(databaseMeta);
        }
        //registry是给每个步骤生成一个标识Id用
        PluginRegistry registry = PluginRegistry.getInstance();
        //第一个表输入步骤(TableInputMeta)
        TableInputMeta tableInput = new TableInputMeta();
        String tableInputPluginId = registry.getPluginId(StepPluginType.class, tableInput);
        //给表输入添加一个DatabaseMeta连接数据库
        DatabaseMeta database_bjdt = transMeta.findDatabase("from");
        tableInput.setDatabaseMeta(database_bjdt);
        String select_sql = "SELECT "+ from_columns +"  FROM "+from_tablename ;
        tableInput.setSQL(select_sql);

        //添加TableInputMeta到转换中
        StepMeta tableInputMetaStep = new StepMeta(tableInputPluginId,"table input",tableInput);
        //给步骤添加在spoon工具中的显示位置
        tableInputMetaStep.setDraw(true);
        tableInputMetaStep.setLocation(100, 100);
        transMeta.addStep(tableInputMetaStep);

        //第二个步骤插入与更新
        InsertUpdateMeta insertUpdateMeta = new InsertUpdateMeta();
        String insertUpdateMetaPluginId = registry.getPluginId(StepPluginType.class,insertUpdateMeta);
        //添加数据库连接
        DatabaseMeta database_kettle = transMeta.findDatabase("to");
        insertUpdateMeta.setDatabaseMeta(database_kettle);
        //设置操作的表
        insertUpdateMeta.setTableName(to_tablename);
        // 设置用来查询的关键字，from表主键
        insertUpdateMeta.setKeyLookup(from_column_key);
        // to表主键
        insertUpdateMeta.setKeyStream(to_column_key);
        //一定要加上
        insertUpdateMeta.setKeyStream2(new String[]{""});
        insertUpdateMeta.setKeyCondition(new String[]{"="});

        //设置要更新的字段，from表字段，按照顺序对应
        String[] updatelookup = from_columns_group;
        //设置要更新的流字段，to表字段，按照顺序对应
        String [] updateStream = to_columns_group;
        // 设置是否更新,按照顺序对应
        Boolean[] updateOrNot = {false,true,true,true,true,true,true,true,true,true,true,true};
        // 设置表字段
        insertUpdateMeta.setUpdateLookup(updatelookup);
        // 设置流字段
        insertUpdateMeta.setUpdateStream(updateStream);
        // 设置是否更新
        insertUpdateMeta.setUpdate(updateOrNot);
        String[] lookup = insertUpdateMeta.getUpdateLookup();
        //添加步骤到转换中
        StepMeta insertUpdateStep = new StepMeta(insertUpdateMetaPluginId,"insert_update",insertUpdateMeta);
        insertUpdateStep.setDraw(true);
        insertUpdateStep.setLocation(250,100);
        transMeta.addStep(insertUpdateStep);
        //******************************************************************

        //******************************************************************

        //添加hop把两个步骤关联起来
        transMeta.addTransHop(new TransHopMeta(tableInputMetaStep, insertUpdateStep));
        System.out.println("***********the end************");
        return transMeta;
    }

}