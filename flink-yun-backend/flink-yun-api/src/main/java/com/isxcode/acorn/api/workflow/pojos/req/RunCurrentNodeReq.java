package com.isxcode.acorn.api.workflow.pojos.req;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import lombok.Data;

@Data
public class RunCurrentNodeReq {

	@Schema(description = "作业实例唯一id", example = "sy_ba1f12b5c8154f999a02a5be2373a438")
	@NotEmpty(message = "作业实例id不能为空")
	private String workInstanceId;
}
