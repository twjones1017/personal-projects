package ms3.project;

import java.io.*;
import java.util.*;
import java.sql.*;


/**
 * @author TWJones01
 */
public class MS3Project {
    

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args){
        
        String fileName = "MS3_DataSet.csv";
        BufferedReader br = null;
        String line = "";
        boolean TitleRowExists;
        int numCorrectColumns = 8;
        int incData = 0;
        String[] data = null;
        List<DBData> Data = new ArrayList<DBData>();
        
        try{
            
            br = new BufferedReader(new FileReader(fileName));
            if(TitleRowExists = true){
                String titleRow = br.readLine();
                
                if(titleRow == null || titleRow.isEmpty()){
                    throw new FileNotFoundException("No CSV file found. Please give"
                            + "a properly formatted file.");
                }
            }
            
            while((line = br.readLine()) != null) {
                
                data = line.split(",");
                
                if(data.length > 0 && data.length == numCorrectColumns){
                    
                    DBData info = new DBData(data[0], data[1],
                    data[2], data[3], data[4], data[5],
                    Float.parseFloat(data[6]), data[7]);
                    Data.add(info);
                } else{
                    incData++;
                } 
            }   
            insertCSVToDB(Data);
               
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        
    }
    
    public static void insertCSVToDB(List<DBData> Data){
        
        String jdbcURL = "jdbc:mysql://localhost:3306/sales";
        String username = "user";
        String password = "password";
        
        String csvFile = "MS3_DataSet.csv";
        
        int batchSize = 20;
        Connection connection = null;
        
        try{
            
            connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);
            String sql = "INSERT INTO review (firstName, lastName, email, sex, address, cardType, amount, location)";
            PreparedStatement statement = connection.prepareStatement(sql);
            
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String lineText = null;
            
            int count = 0;
            
            br.readLine();
            
            while((lineText = br.readLine()) != null){
                
                String[] data = lineText.split(",");
                String firstName = data[0];
                String lastName = data[1];
                String email = data[2];
                String sex = data[3];
                String address = data[4];
                String cardType = data[5];
                String amount = data[6];
                String location = data[7];
                
                statement.setString(1, firstName);
                statement.setString(2, lastName);
                statement.setString(3, email);
                statement.setString(4, sex);
                statement.setString(5, address);
                statement.setString(6, cardType);
                
                Float dAmount = Float.parseFloat(amount);
                statement.setFloat(7, dAmount);
                
                statement.setString(8, location);
                statement.addBatch();
                
                if(count % batchSize == 0){
                    statement.executeBatch();
                }
            }
            
            br.close();
            statement.executeBatch();
            connection.commit();
            connection.close();
            
            
        }catch (IOException e){
            System.err.println(e);
        }catch (SQLException e){
            System.err.println(e);
        }
        
        try{
            connection.rollback();
        }catch(SQLException e){
            e.printStackTrace();
        }  
    } 
}
