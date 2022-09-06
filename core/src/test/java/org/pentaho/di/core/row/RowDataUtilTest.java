/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.core.row;

import junit.framework.TestCase;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.value.ValueMetaInteger;
import org.pentaho.di.core.row.value.ValueMetaString;

import java.util.ArrayList;
import java.util.List;

public class RowDataUtilTest extends TestCase {
  public boolean arrayCompare( Object[] arr1, int start1, Object[] arr2, int start2, int len ) {
    boolean rcode = true;

    int idx = 0;
    while ( idx < len && rcode == true ) {
      if ( arr1[start1 + idx] == null && arr2[start2 + idx] == null ) {
        // do nothing
      } else if ( !arr1[start1 + idx].equals( arr2[start2 + idx] ) ) {
        rcode = false;
      }
      idx++;
    }

    return rcode;
  }

  public void testResizeArray() throws KettleValueException {
    Object[] arr1 = new Object[] { "test", Boolean.TRUE, new Long( 100 ), new Long( 101 ), new String( "test1" ) };
    Object[] nullArr =
      new Object[] {
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null };

    // ------- Normal cases

    // Make array size bigger
    Object[] arr2 = RowDataUtil.resizeArray( arr1, 7 );
    assertTrue( arr1 != arr2 ); // Allocate new array
    assertTrue( arr2.length >= 7 );
    assertTrue( arrayCompare( arr1, 0, arr2, 0, arr1.length ) ); // Elements of arr1 are copied
    assertTrue( arrayCompare( arr2, arr1.length, nullArr, 0, arr2.length - arr1.length ) ); // last are null

    // Make array size smaller
    Object[] arr3 = RowDataUtil.resizeArray( arr1, 3 );
    assertTrue( arr3 == arr1 );

    // Make array size equal
    Object[] arr4 = RowDataUtil.resizeArray( arr1, arr1.length );
    assertTrue( arr1 != arr2 ); // Allocatew new array
    assertEquals( arr4.length, arr1.length );
    assertTrue( arrayCompare( arr1, 0, arr4, 0, arr1.length ) ); // Elements of arr1 are copied
  }

  public void testRemoveItem() throws KettleValueException {
    Object[] arr = new Object[] { new Long( 1L ), new Long( 2L ), new Long( 3L ), new Long( 4L ), new Long( 5L ) };

    // Remove the first item
    Object[] comp1 = new Object[] { new Long( 2L ), new Long( 3L ), new Long( 4L ), new Long( 5L ) };
    Object[] newArr1 = RowDataUtil.removeItem( arr, 0 );
    assertTrue( newArr1.length == comp1.length );
    assertTrue( newArr1.length == arr.length - 1 );
    assertTrue( arrayCompare( newArr1, 0, comp1, 0, newArr1.length ) );

    // Remove the last item
    Object[] comp2 = new Object[] { new Long( 2L ), new Long( 3L ), new Long( 4L ) };
    Object[] newArr2 = RowDataUtil.removeItem( newArr1, newArr1.length - 1 );
    assertTrue( newArr2.length == arr.length - 2 );
    assertTrue( newArr2.length == comp2.length );
    assertTrue( arrayCompare( newArr2, 0, comp2, 0, newArr2.length ) );

    // Remove in the middle
    Object[] comp3 = new Object[] { new Long( 2L ), new Long( 4L ) };
    Object[] newArr3 = RowDataUtil.removeItem( newArr2, 1 );
    assertTrue( newArr3.length == arr.length - 3 );
    assertTrue( newArr3.length == comp3.length );
    assertTrue( arrayCompare( newArr3, 0, comp3, 0, newArr3.length ) );

    // Remove until empty
    Object[] comp4 = new Object[] {};
    Object[] newArr4 = RowDataUtil.removeItem( newArr3, 0 );
    newArr4 = RowDataUtil.removeItem( newArr4, 0 );
    assertTrue( newArr4.length == arr.length - 5 );
    assertTrue( newArr4.length == comp4.length );
    assertTrue( arrayCompare( newArr4, 0, comp4, 0, newArr4.length ) );
  }

  public void testAddRowData() throws KettleValueException {
    Object[] arr = new Object[] { new Long( 1L ), new Long( 2L ), new Long( 3L ) };

    // Do all different combinations of adding rows to
    // each other
    Object[] newArr1 =
      RowDataUtil.addRowData(
        new Object[] {}, 0, new Object[] { new Long( 1L ), new Long( 2L ), new Long( 3L ) } );
    assertTrue( newArr1.length >= arr.length );
    assertTrue( arrayCompare( newArr1, 0, arr, 0, arr.length ) );

    Object[] newArr2 =
      RowDataUtil.addRowData(
        new Object[] { new Long( 1L ), new Long( 2L ), new Long( 3L ) }, 3, new Object[] {} );
    assertTrue( newArr2.length >= arr.length );
    assertTrue( arrayCompare( newArr2, 0, arr, 0, arr.length ) );

    Object[] newArr3 =
      RowDataUtil.addRowData(
        new Object[] { new Long( 1L ) }, 1, new Object[] { new Long( 2L ), new Long( 3L ) } );
    assertTrue( newArr3.length >= arr.length );
    assertTrue( arrayCompare( newArr3, 0, arr, 0, arr.length ) );

    Object[] newArr4 =
      RowDataUtil.addRowData(
        new Object[] { new Long( 1L ), new Long( 2L ) }, 2, new Object[] { new Long( 3L ) } );
    assertTrue( newArr4.length >= arr.length );
    assertTrue( arrayCompare( newArr4, 0, arr, 0, arr.length ) );
  }

  public void testAddValueData() throws KettleValueException {
    Object[] arr1 = new Object[] { new Long( 1L ) };
    Object[] arr2 = new Object[] { new Long( 1L ), new Long( 2L ) };

    Object[] newArr1 = RowDataUtil.addValueData( new Object[] {}, 0, new Long( 1L ) );
    assertTrue( newArr1.length >= arr1.length );
    assertTrue( arrayCompare( newArr1, 0, arr1, 0, arr1.length ) );

    Object[] newArr2 = RowDataUtil.addValueData( new Object[] { new Long( 1L ) }, 1, new Long( 2L ) );
    assertTrue( newArr2.length >= arr2.length );
    assertTrue( arrayCompare( newArr2, 0, arr2, 0, arr2.length ) );
  }

  public void testRemoveItems() throws KettleValueException {
    Object[] arr1 =
      new Object[] { new Long( 1L ), new Long( 2L ), new Long( 3L ), new Long( 4L ), new Long( 5L ) };
    Object[] comp1 = new Object[] { new Long( 2L ), new Long( 4L ) };

    Object[] newArr1 = RowDataUtil.removeItems( arr1, new int[] {} );
    assertTrue( newArr1.length == arr1.length );
    assertTrue( arrayCompare( newArr1, 0, arr1, 0, newArr1.length ) );

    Object[] newArr2 = RowDataUtil.removeItems( arr1, new int[] { 0, 2, 4 } );
    assertTrue( newArr2.length == arr1.length - 3 );
    assertTrue( arrayCompare( newArr2, 0, comp1, 0, newArr2.length ) );
  }

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

  public void testCreateCustomResizedCopyWithDifferentColumnCount() {
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

  public void testCreateCustomResizedCopyWithDifferentColumnValue() {
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

  public void testCreateCustomResizedCopyWithEmptyRow() {
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
