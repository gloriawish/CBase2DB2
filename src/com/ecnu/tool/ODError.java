/*    */ package com.ecnu.tool;
/*    */ 
/*    */ public enum ODError
/*    */ {
/* 14 */   SUCCESS, 
/*    */ 
/* 18 */   ERROR, 
/*    */ 
/* 21 */   ERROR_EXCEPTION, 
/*    */ 
/* 25 */   ERROR_PATTERN, 
/*    */ 
/* 28 */   ERROR_UNKNOWN_ITEM, 
/*    */ 
/* 31 */   ERROR_EMPTY_VALUE, 
/*    */ 
/* 34 */   ERROR_WRONG_VALUE, 
/*    */ 
/* 37 */   ERROR_MISS_ITEM, 
/*    */ 
/* 40 */   ERROR_CONFLICT;
/*    */ 
/*    */   public boolean isSuccess()
/*    */   {
/* 49 */     return this == SUCCESS;
/*    */   }
/*    */ 
/*    */   public boolean isError()
/*    */   {
/* 57 */     return !isSuccess();
/*    */   }
/*    */ }

/* Location:           D:\oceanbase-deployer-1.0.jar
 * Qualified Name:     com.lbzhong.odeployer.common.ODError
 * JD-Core Version:    0.6.0
 */