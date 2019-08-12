package com.hc.pdb.state;

/**
 * IRecorverable
 * 可自动恢复的
 * 断电后能够自动恢复自己的状态，以达成一致性。
 * @author han.congcong
 * @date 2019/8/10
 */

public interface IRecoveryable {
    /**
     * 恢复自身状态
     * @throws RecorverFailedException 恢复失败报错
     */
    void recovery() throws RecorverFailedException;
}
