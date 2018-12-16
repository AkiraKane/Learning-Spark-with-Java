package project4.mappers;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import project4.pojos.House;

import java.text.SimpleDateFormat;

public class HouseMapper implements MapFunction<Row, House> {

    @Override
    public House call(Row row) throws Exception {

        House h = new House();
        h.setId(row.getAs("id"));
        h.setAddress(row.getAs("address"));
        h.setSqft(row.getAs("sqft"));
        h.setPrice(row.getAs("price"));
        // h.setVacantBy(row.getAs("vacantBy"));


        String vacancyDateString = row.getAs("vacantBy").toString();

        if(vacancyDateString != null){
            SimpleDateFormat parser = new SimpleDateFormat("yyyy-mm-dd");
            h.setVacantBy(parser.parse(vacancyDateString));
        }

        return h;
    }
}
