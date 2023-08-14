package com.jia.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: WaterSensor
 * Package: com.jia.flink.pojo
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/11 20:48
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {
	private String id;
	private Integer vc;
	private Long ts;

}
