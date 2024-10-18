import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryToMapList {
    public static void main(String[] args) {
        try (BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService()) {
            // SQL查询语句
            String query = "SELECT * FROM `your-project.your_dataset.your_table`";
            
            // 配置查询
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
            
            // 执行查询
            TableResult results = bigquery.query(queryConfig);
            
            // 将结果转换为List<Map<String, Object>>
            List<Map<String, Object>> resultList = new ArrayList<>();
            for (FieldValueList row : results.iterateAll()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < row.size(); i++) {
                    FieldValue fieldValue = row.get(i);
                    Schema.Field field = results.getSchema().getField(i);
                    
                    if (field.getType().getStandardType() == StandardSQLTypeName.STRUCT) {
                        // 处理嵌套字段
                        map.put(field.getName(), convertStructToListOfMaps(fieldValue));
                    } else if (field.getType().getMode() == Schema.Field.Mode.REPEATED) {
                        // 处理可重复字段
                        map.put(field.getName(), convertRepeatedToList(fieldValue));
                    } else {
                        // 处理基本类型
                        map.put(field.getName(), fieldValue.getValue());
                    }
                }
                resultList.add(map);
            }

            // 输出结果
            for (Map<String, Object> map : resultList) {
                System.out.println(map);
            }
        } catch (BigQueryException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static List<Map<String, Object>> convertStructToListOfMaps(FieldValue fieldValue) {
        List<Map<String, Object>> listOfMaps = new ArrayList<>();
        for (FieldValue structValue : fieldValue.getRecordValue()) {
            Map<String, Object> map = new HashMap<>();
            for (int j = 0; j < structValue.getRecordValue().size(); j++) {
                map.put(structValue.getRecordValue().get(j).getName(), 
                         structValue.getRecordValue().get(j).getValue());
            }
            listOfMaps.add(map);
        }
        return listOfMaps;
    }

    private static List<Object> convertRepeatedToList(FieldValue fieldValue) {
        List<Object> list = new ArrayList<>();
        for (FieldValue repeatedValue : fieldValue.getRepeatedValue()) {
            if (repeatedValue.getType().getStandardType() == StandardSQLTypeName.STRUCT) {
                list.add(convertStructToListOfMaps(repeatedValue));
            } else {
                list.add(repeatedValue.getValue());
            }
        }
        return list;
    }
}
