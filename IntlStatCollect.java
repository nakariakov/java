import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.BufferedWriter;
import java.io.FileWriter;
 
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;




public class IntlStatCollect {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		InputStreamReader meatIr = null;
		String agency = "IMF";
		String type =null;
		String ticker = null;
		String format = "json";
		 String testmode = "";
//		String testmode = "&offset=0&limit=1";
		
		JSONParser jsonParser = new JSONParser();
		Date dt = new Date();
		System.out.println(dt.toString());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
		
		ArrayList<String> tickers = new ArrayList<String>();
		tickers.add("IMF_IFS_D563");
		tickers.add("IMF_IFS_C495");
		tickers.add("IMF_IFS_C491");
		tickers.add("IMF_IFS_C146");
		tickers.add("IMF_IFS_D1155");
		tickers.add("IMF_IFS_D1162");
		tickers.add("IMF_IFS_D1160");
		tickers.add("IMF_IFS_D1158");
		tickers.add("IMF_IFS_C347");
		tickers.add("IMF_BOP_E518");
		tickers.add("IMF_BOP_F552");
		tickers.add("IMF_BOP_F596");
		tickers.add("IMF_IFS_D55");
		tickers.add("IMF_IFS_G30");
		tickers.add("IMF_IFS_E104");
		tickers.add("IMF_IFS_C76");
		
		for (int i = 0; i < tickers.size() ; i++) {
			try {
				
				ticker = tickers.get(i);
				
				// API categories
		        type ="categories";
				URL metaURL = new URL("http://edw2.boknet.intra/api/v1/"+agency+"/"+type+"/"+ticker+"/?format="+format + testmode);// 호출할 url
				System.out.println(metaURL);
		        String fileName = "C:\\ECOS\\workspace\\TEST\\" + ticker + "_" + sdf.format(dt).toString()+".tsv" ;
		        
				BufferedWriter fw = new BufferedWriter(new FileWriter(fileName, true));
				// File Header 
				fw.write("seriesId" +"\t"+"seriesName"+ "\t"+"freq"+ "\t"+"area"+ "\t"+"unit"+ "\t" +"dataPeriod"+"\t"+"dataValue"+"\n");
	
				
				HttpURLConnection mataCon = (HttpURLConnection)metaURL.openConnection();
				mataCon.setRequestMethod("GET");
				meatIr = new InputStreamReader(mataCon.getInputStream(), "UTF-8");
				
				JSONObject catObj =(JSONObject)jsonParser.parse(meatIr);
				JSONArray catArr = (JSONArray)catObj.get("series");
				
				
				String seriesId = null;
				String seriesName = null;
				String seriesLink = null;
				String freq = null;
				String area = null;
				String unit = null;
				
				String dataPeriod = null;
				String dataValue = null;
				
				
				for( int j =0 ; j < catArr.size() ; j++ ){
					JSONObject catSeriesObj = (JSONObject)catArr.get(j);
					
					seriesId = catSeriesObj.get("id").toString();
					seriesName = catSeriesObj.get("name").toString();
					
					JSONArray linkArr = (JSONArray)catSeriesObj.get("links");
					for (int k = 0; k < linkArr.size(); k++) {
						JSONObject linkObj = (JSONObject)linkArr.get(k);
						seriesLink = linkObj.get("href").toString();
					}
					
					// API series
					type ="series";
					URL seriesURL = new URL(seriesLink + "/?format="+format + testmode);// 호출할 url
					HttpURLConnection seriesCon = (HttpURLConnection)seriesURL.openConnection();
					seriesCon.setRequestMethod("GET");
					InputStreamReader seriesIr = new InputStreamReader(seriesCon.getInputStream(), "UTF-8");
					JSONObject seriesObj =(JSONObject)jsonParser.parse(seriesIr);
					freq = seriesObj.get("freq").toString();
					area = seriesObj.get("area").toString();
					unit = seriesObj.get("unit").toString();
					
					
					// API observations 
					type ="observations";
					URL dataURL = new URL("http://edw2.boknet.intra/api/v1/" + agency + "/"+ type +"/" + catSeriesObj.get("id") + "/?format="+format+ testmode);// 호출할 url
					System.out.println(dataURL);

					HttpURLConnection conDataCat = (HttpURLConnection)dataURL.openConnection();
					conDataCat.setRequestMethod("GET");
					InputStreamReader irData = new InputStreamReader(conDataCat.getInputStream(), "UTF-8");
					JSONArray dataArr =(JSONArray)jsonParser.parse(irData);
					Iterator dataIter = dataArr.iterator();
					while (dataIter.hasNext()) {
						JSONObject obs = (JSONObject)dataIter.next();
						dataPeriod = obs.get("period").toString();
						dataValue= obs.get("value").toString();
						fw.write(seriesId +"\t"+seriesName+ "\t"+freq+ "\t"+area+ "\t"+unit+ "\t" +dataPeriod+"\t"+dataValue+"\n");
					}
				}
		        fw.flush();
				fw.close();
				
				
				
			
			} catch(Exception e) {
				e.printStackTrace();
			} finally {
				if(meatIr != null){
					try {
						meatIr.close();
					} catch(Exception e) {
						e.printStackTrace(); 
					}
				}
			}
		}

		System.out.println("All Processed!!");

	}
}
