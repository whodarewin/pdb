package com.hc.pdb.state;

import java.util.List;

/**
 * ILogRecorder
 * 任务的执行是change状态，而不是顺序打log日志。
 * 任务名称：任务状态：任务参数（此参数与状态相关，包括启动参数和运行到每一个stage的参数）
 * @author han.congcong
 * @date 2019/8/9
 */

public interface IStatusLogRecorder {

    void log(StatusLog log);

    /**
     * 状态记录
     */
    class StatusLog{
        /**
         * 名称
         */
        private String name;
        /**
         * 状态
         */
        private String status;
        /**
         * 参数
         */
        private List<String> params;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public List<String> getParams() {
            return params;
        }

        public void setParams(List<String> params) {
            this.params = params;
        }
    }
}
