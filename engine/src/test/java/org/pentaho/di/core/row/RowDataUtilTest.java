package org.pentaho.di.core.row;

import org.junit.Test;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RowDataUtilTest {

    @Test
    public void testCreateCustomResizedCopy() {
        final Object[][] objects = {{1, "Enrique", "test 1"},{1,"Enrique","test 2"}} ;

        final RowMetaInterface row1Meta = new RowMeta();
        row1Meta.addValueMeta(new ValueMetaInteger("id"));
        row1Meta.addValueMeta(new ValueMetaString("first_name"));
        row1Meta.addValueMeta(new ValueMetaString("test_1"));

        final RowMetaInterface row2Meta = new RowMeta();
        row2Meta.addValueMeta(new ValueMetaInteger("id"));
        row2Meta.addValueMeta(new ValueMetaString("first_name"));
        row2Meta.addValueMeta(new ValueMetaString("test_2"));

        final RowMetaInterface[] inputRowMetas = { row1Meta, row2Meta };

        final List<ValueMetaInterface> outputMetaList = new ArrayList<>();
        outputMetaList.add(new ValueMetaInteger("id"));
        outputMetaList.add(new ValueMetaString("first_name"));
        outputMetaList.add(new ValueMetaString("test_1"));
        outputMetaList.add(new ValueMetaString("test_2"));

        final Object[] result = RowDataUtil.createCustomResizedCopy(objects, outputMetaList, inputRowMetas);
        assertEquals( 1, result[0] );
        assertEquals( "Enrique", result[1] );
        assertEquals( "test 1", result[2] );
        assertEquals( "test 2", result[3] );
    }

    @Test
    public void testCreateCustomResizedCopyDifferentColumnNumber() {
        final Object[][] objects = {{1, "Enrique", "test 1"},{1,"Enrique", "Smith", "test 2"}} ;

        final RowMetaInterface row1Meta = new RowMeta();
        row1Meta.addValueMeta(new ValueMetaInteger("id"));
        row1Meta.addValueMeta(new ValueMetaString("first_name"));
        row1Meta.addValueMeta(new ValueMetaString("test_1"));

        final RowMetaInterface row2Meta = new RowMeta();
        row2Meta.addValueMeta(new ValueMetaInteger("id"));
        row2Meta.addValueMeta(new ValueMetaString("first_name"));
        row2Meta.addValueMeta(new ValueMetaString("last_name"));
        row2Meta.addValueMeta(new ValueMetaString("test_2"));

        final RowMetaInterface[] inputRowMetas = { row1Meta, row2Meta };

        final List<ValueMetaInterface> outputMetaList = new ArrayList<>();
        outputMetaList.add(new ValueMetaInteger("id"));
        outputMetaList.add(new ValueMetaString("first_name"));
        outputMetaList.add(new ValueMetaString("last_name"));
        outputMetaList.add(new ValueMetaString("test_1"));
        outputMetaList.add(new ValueMetaString("test_2"));

        final Object[] result = RowDataUtil.createCustomResizedCopy(objects, outputMetaList, inputRowMetas);
        assertEquals( 1, result[0] );
        assertEquals( "Enrique", result[1] );
        assertEquals( "Smith", result[2] );
        assertEquals( "test 1", result[3] );
        assertEquals( "test 2", result[4] );
    }

    @Test
    public void testCreateCustomResizedCopyDifferentFirstNameValue() {
        final Object[][] objects = {{1, "Enrique", "test 1"},{1,"Jaishree", "test 2"}} ;

        final RowMetaInterface row1Meta = new RowMeta();
        row1Meta.addValueMeta(new ValueMetaInteger("id"));
        row1Meta.addValueMeta(new ValueMetaString("first_name"));
        row1Meta.addValueMeta(new ValueMetaString("test_1"));

        final RowMetaInterface row2Meta = new RowMeta();
        row2Meta.addValueMeta(new ValueMetaInteger("id"));
        row2Meta.addValueMeta(new ValueMetaString("first_name"));
        row2Meta.addValueMeta(new ValueMetaString("test_2"));

        final RowMetaInterface[] inputRowMetas = { row1Meta, row2Meta };

        final List<ValueMetaInterface> outputMetaList = new ArrayList<>();
        outputMetaList.add(new ValueMetaInteger("id"));
        outputMetaList.add(new ValueMetaString("first_name"));
        outputMetaList.add(new ValueMetaString("test_1"));
        outputMetaList.add(new ValueMetaString("test_2"));

        final Object[] result = RowDataUtil.createCustomResizedCopy(objects, outputMetaList, inputRowMetas);
        assertEquals( 1, result[0] );
        assertEquals( "Jaishree", result[1] );
        assertEquals( "test 1", result[2] );
        assertEquals( "test 2", result[3] );
    }

    @Test
    public void testCreateCustomResizedCopyWhenRowMissing() {
        final Object[][] objects = {{null,null,null},{1,"Jaishree", "test 2"}} ;

        final RowMetaInterface row1Meta = new RowMeta();
        row1Meta.addValueMeta(new ValueMetaInteger("id"));
        row1Meta.addValueMeta(new ValueMetaString("first_name"));
        row1Meta.addValueMeta(new ValueMetaString("test_1"));

        final RowMetaInterface row2Meta = new RowMeta();
        row2Meta.addValueMeta(new ValueMetaInteger("id"));
        row2Meta.addValueMeta(new ValueMetaString("first_name"));
        row2Meta.addValueMeta(new ValueMetaString("test_2"));

        final RowMetaInterface[] inputRowMetas = { row1Meta, row2Meta };

        final List<ValueMetaInterface> outputMetaList = new ArrayList<>();
        outputMetaList.add(new ValueMetaInteger("id"));
        outputMetaList.add(new ValueMetaString("first_name"));
        outputMetaList.add(new ValueMetaString("test_1"));
        outputMetaList.add(new ValueMetaString("test_2"));

        final Object[] result = RowDataUtil.createCustomResizedCopy(objects, outputMetaList, inputRowMetas);
        assertEquals( 1, result[0] );
        assertEquals( "Jaishree", result[1] );
        assertEquals( null, result[2] );
        assertEquals( "test 2", result[3] );
    }

}
