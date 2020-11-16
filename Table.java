package com.ELTTool;

import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import java.awt.*;
import java.util.concurrent.*;
import java.util.logging.Logger;


/**
 * This call is the super for all tables used in the app and keep the content up to date every second
 */
public abstract class Table {


    private static Logger logger = Logger.getLogger(Table.class.getName());

    public JTable table;
    public AbstractTableModel tableModel;
    public Object[][] data;

    public Scheduler sched;








    public abstract Object[][] getData();
    public abstract String[] getHeaders();
    public abstract void setColumnWidths();
    public abstract void setTableSize(int width, int height);




    public JTable getTable() {


        try {


            data = getData();
            tableModel = new AbstractTableModel() {

                @Override
                public String getColumnName(int col) {
                    return getHeaders()[col];
                }


                @Override
                public int getRowCount() {

                    if (data == null)
                        return 0;
                    else
                        return data.length;

                }

                @Override
                public int getColumnCount() {

                    if ( data == null)
                        return 0;
                    else
                        return data[0].length;
                }

                @Override
                public Object getValueAt(int i, int i1) {
                    return data[i][i1];
                }

            };



            table = new JTable(tableModel);
            table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
            table.setAutoCreateRowSorter(true);
            table.setBorder(BorderFactory.createLineBorder(Color.LIGHT_GRAY));
            setColumnWidths();

            sched = new Scheduler(new RefreshData(), 2000L, 2000L, null, null, null);
            sched.runScheduler();

            return table;



        } catch (Exception e) {
            logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
            return null;

        }



    }





    class RefreshData implements Runnable {


        @Override
        public void run() {

            try {


                Object[][] newData = getData();

                int tabRC = data == null ? 0 : data.length;
                int curRC = newData.length;

                //logger.info("tabRC: " + tabRC + " | curRC: " + curRC + " | tableRowCount: " + table.getRowCount());

                // if the row counts are the same then just update the table
                // else just wipe it clean and restate

                if (tabRC == curRC) {


                    for(int r = 0; r < table.getRowCount(); r++ )
                        for (int c = 0; c < table.getColumnCount(); c++) {
                            if(data[r][c] != newData[r][c]) {

                                //logger.info("Table data change new: " + newData[r][c] + " | old: " + data[r][c] + " | r c: " + r + " " + c);
                                data[r][c] = newData[r][c];
                                tableModel.fireTableCellUpdated(r, c);
                            }

                        }





                } else {

                    //---------------------------------------------------------------
                    // get the ones that don't show up in the new array=


                    if (tableModel.getRowCount() > 0)
                        tableModel.fireTableRowsDeleted(0, tableModel.getRowCount() - 1);


                    data = newData;

                    for (int i = 0; i < data.length; i++)
                        tableModel.fireTableRowsInserted(i, i);


                }


            } catch (Exception e) {
                logger.severe(LoggingUtils.getErrorEntry(e, null, null, null));
            }




        }





    };







}
