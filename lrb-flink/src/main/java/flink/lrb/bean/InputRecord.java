package flink.lrb.bean;

import java.io.Serializable;

public class InputRecord implements Serializable {

	private int	m_iType;   // Type:
				   //	. 0: position report
				   //	. 2: account balance request
				   //	. 3: daily expenditure request
				   //	. 4: travel time request
	private int	m_iTime;   // 0...10799 (second), timestamp position report emitted
	private int	m_iVid;	   // 0...MAXINT, vehicle identifier
	private int	m_iSpeed;  // 0...100, speed of the vehicle
	private int	m_iXway;   // 0...L-1, express way
	private int	m_iLane;   // 0...4, lane
	private int	m_iDir;    // 0..1, direction
	private int	m_iSeg;    // 0...99, segment
	private int	m_iPos;    // 0...527999, position of the vehicle
	private int	m_iQid;    // query identifier
	private int m_iSinit;  // start segment
	private int	m_iSend;   // end segment
	private int	m_iDow;    // 1..7, day of week
	private int	m_iTod;    // 1...1440, minute number in the day
	private int	m_iDay;    // 1..69, 1: yesterday, 69: 10 weeks ago


	public InputRecord(String line) {
		String[] tokens = line.split(",");
		m_iType		= Integer.parseInt(tokens[0]);
		m_iTime		= Integer.parseInt(tokens[1]);   // 0...10799 (second), timestamp position report emitted
		m_iVid		= Integer.parseInt(tokens[2]);	   // 0...MAXINT, vehicle identifier
		m_iSpeed	= Integer.parseInt(tokens[3]);  // 0...100, speed of the vehicle
		m_iXway		= Integer.parseInt(tokens[4]);	// 0...L-1, express way
		m_iLane		= Integer.parseInt(tokens[5]);   // 0...4, lane
		m_iDir		= Integer.parseInt(tokens[6]);    // 0..1, direction
		m_iSeg		= Integer.parseInt(tokens[7]);    // 0...99, segment
		m_iPos		= Integer.parseInt(tokens[8]);    // 0...527999, position of the vehicle
		m_iQid		= Integer.parseInt(tokens[9]);    // query identifier
		m_iSinit	= Integer.parseInt(tokens[10]);  // start segment
		m_iSend		= Integer.parseInt(tokens[11]);   // end segment
		m_iDow		= Integer.parseInt(tokens[12]);    // 1..7, day of week
		m_iTod		= Integer.parseInt(tokens[13]);    // 1...1440, minute number in the day
		m_iDay		= Integer.parseInt(tokens[14]);    // 1..69, 1: yesterday, 69: 10 weeks ago
	}

	@Override
	public String toString() {
		return "InputRecord{" +
				"m_iType=" + m_iType +
				", m_iTime=" + m_iTime +
				", m_iVid=" + m_iVid +
				", m_iSpeed=" + m_iSpeed +
				", m_iXway=" + m_iXway +
				", m_iLane=" + m_iLane +
				", m_iDir=" + m_iDir +
				", m_iSeg=" + m_iSeg +
				", m_iPos=" + m_iPos +
				", m_iQid=" + m_iQid +
				", m_iSinit=" + m_iSinit +
				", m_iSend=" + m_iSend +
				", m_iDow=" + m_iDow +
				", m_iTod=" + m_iTod +
				", m_iDay=" + m_iDay +
				'}';
	}

	public int getM_iType() {
		return m_iType;
	}

	public int getM_iTime() {
		return m_iTime;
	}

	public int getM_iVid() {
		return m_iVid;
	}

	public int getM_iSpeed() {
		return m_iSpeed;
	}

	public int getM_iXway() {
		return m_iXway;
	}

	public int getM_iLane() {
		return m_iLane;
	}

	public int getM_iDir() {
		return m_iDir;
	}

	public int getM_iSeg() {
		return m_iSeg;
	}

	public int getM_iPos() {
		return m_iPos;
	}

	public int getM_iQid() {
		return m_iQid;
	}

	public int getM_iSinit() {
		return m_iSinit;
	}

	public int getM_iSend() {
		return m_iSend;
	}

	public int getM_iDow() {
		return m_iDow;
	}

	public int getM_iTod() {
		return m_iTod;
	}

	public int getM_iDay() {
		return m_iDay;
	}
}
