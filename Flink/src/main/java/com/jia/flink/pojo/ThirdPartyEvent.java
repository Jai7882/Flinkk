package com.jia.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: ThirdPartyEvent
 * Package: com.jia.flink.pojo
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 20:03
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ThirdPartyEvent {
	private String orderId;

	private String eventType;

	private String thirdPartyName;

	private Long ts;
}
