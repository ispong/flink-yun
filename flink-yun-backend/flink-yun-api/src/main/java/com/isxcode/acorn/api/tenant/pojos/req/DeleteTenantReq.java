package com.isxcode.acorn.api.tenant.pojos.req;

import io.swagger.v3.oas.annotations.media.Schema;
import javax.validation.constraints.NotEmpty;
import lombok.Data;

@Data
public class DeleteTenantReq {

    @Schema(title = "租户唯一id", example = "fy_123456789")
    @NotEmpty(message = "租户id不能为空")
    private String tenantId;
}
