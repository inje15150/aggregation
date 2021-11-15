package entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Cpu {
    private int count;
    private float sum;
    private float avg;
    private float max;
    private float min;
}
