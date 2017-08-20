package com.csr.datamanager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.fleety.server.BasicServer;
import com.fleety.util.pool.db.DbConnPool.DbHandle;
import com.fleety.util.pool.db.DbConnPool.StatementHandle;
import com.labServer.model.LabDisprobeNumber;
import com.labServer.model.LabModify;

import server.db.DbServer;

public class DateInitServer extends BasicServer{
	
	
	private static DateInitServer instance               = null;

    public static DateInitServer getSingleInstance()
    {
        if (instance == null)
        {
            instance = new DateInitServer();
        }
        return instance;
    }    
    public HashMap<String, LabDisprobeNumber> labDisprobeNumber=new HashMap<>();
    HashMap<String, LabModify> modMap = new HashMap<String, LabModify>();
	
	public HashMap Getmodifys(){
		return modMap;
	}
	@Override
	public boolean startServer() {
		String init_delay = this.getStringPara("init_delay");
		System.out.println("数据缓存刷新：：");
		init();
		return true;
	}

	private void init() {
			DbHandle con = DbServer.getSingleInstance().getConn();
			StatementHandle stmt;
			String sql = "select * from lab_disprobenumber";				
			try {
				stmt = con.prepareStatement(sql);
				ResultSet	rs = stmt.executeQuery();			
					while (rs.next()) {
						LabDisprobeNumber ld = new LabDisprobeNumber();
						ld.setInputProbeNumber(rs.getString("inputProbeNumber"));
						ld.setDisplayProbeNumber(rs.getString("displayProbeNumber"));
						ld.setTab_InputName(rs.getString("tab_InputName"));
						ld.setTab_DisplayName(rs.getString("tab_DisplayName"));						
						this.labDisprobeNumber.put(ld.getInputProbeNumber(), ld);		
					}
					System.out.println("初始化探头信息个数：："+labDisprobeNumber.size());
			} catch (SQLException e) {
				e.printStackTrace();
			}	
			
			
			
			
					
				
				StatementHandle stmt2;
				String sql2 = "select * from lab_modify";
				try {
					stmt2 = con.prepareStatement(sql2);
					ResultSet rs2 = stmt2.executeQuery();									
					while (rs2.next()) {
						LabModify lm = new LabModify();
						lm.setInputProbeNumber(rs2.getString("inputProbeNumber"));
						lm.setDisProbeNumber(rs2.getString("disProbeNumber"));
						lm.setModifyTemp(rs2.getDouble("modifyTemp"));
						lm.setModifyHum(rs2.getDouble("modifyHum"));	
						modMap.put(lm.getInputProbeNumber(), lm);
					}		
					DbServer.getSingleInstance().releaseConn(con);
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
			
	}
}
