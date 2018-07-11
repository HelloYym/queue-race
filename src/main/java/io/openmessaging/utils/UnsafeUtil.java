package io.openmessaging.utils;

import com.sun.deploy.util.ReflectionUtil;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Created with IntelliJ IDEA.
 * Description:
 * User: chenyifan
 * Date: 2018-07-11
 * Time: 下午2:19
 */
public class UnsafeUtil {

    public static Unsafe UNSAFE;

    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE =  (Unsafe) f.get(null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
