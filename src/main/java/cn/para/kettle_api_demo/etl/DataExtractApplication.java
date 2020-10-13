package cn.para.kettle_api_demo.etl;

import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransHopMeta;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.tableinput.TableInputMeta;
import org.pentaho.di.trans.steps.tableoutput.TableOutputMeta;

public class DataExtractApplication {

    public static void main(String[] args) throws KettleException {
        String sourceQuerySql = "SELECT TRANCODE, TRANNAME, FLAG FROM kettle_test.ykt_jylx_TRCD;";
        String targetSchemaName = "tangyibo";
        String targetTableName = "ykt_jylx_TRCD";

        KettleEnvironment.init();
        KettleClientEnvironment.getInstance().setClient(KettleClientEnvironment.ClientType.CARTE);

        TransMeta transMeta = new TransMeta();
        transMeta.setName("ktrans");

        DatabaseMeta sourcedb = new DatabaseMeta("source", "mysql", "Native(JDBC)", "172.17.207.210", "kettle_test", "3306", "tangyibo", "tangyibo");
        DatabaseMeta targetdb = new DatabaseMeta("target", "Greenplum", "Native(JDBC)", "172.17.207.151", "study", "5432", "study", "123456");
        sourcedb.addOptions();
        sourcedb.setQuoteAllFields(true);//这里在数据库连接的高级设置中
        targetdb.setQuoteAllFields(true);//这里在数据库连接的高级设置中

        transMeta.addDatabase(sourcedb);
        transMeta.addDatabase(targetdb);

        TableInputMeta t_input = new TableInputMeta();
        t_input.setDatabaseMeta(sourcedb);
        t_input.setSQL(sourceQuerySql);
        StepMeta input = new StepMeta("tableInput", t_input);
        transMeta.addStep(input);

        TableOutputMeta t_output = new TableOutputMeta();
        t_output.setDatabaseMeta(targetdb);
        t_output.setSchemaName(targetSchemaName);
        t_output.setTableName(targetTableName);
        t_output.setCommitSize(50000);
        t_output.setTruncateTable(true);//截断目标表内的数据
        StepMeta output = new StepMeta("tableOutput", t_output);
        transMeta.addStep(output);
        transMeta.addTransHop(new TransHopMeta(input, output));

        //打印转换的XML配置
        String transXml = transMeta.getXML();
        System.out.println("=================");
        System.out.println(transXml);

        Trans trans = new Trans(transMeta);
        try {
            trans.execute(null);
            System.out.println("start............");
            trans.waitUntilFinished();
            System.out.println("end............");
            if (trans.getErrors() > 0) {
                String errMsg = KettleLogStore.getAppender().getBuffer(trans.getLogChannelId(), false).toString();
                throw new KettleException(errMsg);
            }
        } finally {
            trans.cleanup();
        }

    }

}