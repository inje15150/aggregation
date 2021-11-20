package entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Memory {
    private int count;
    private float sum;
    private float avg;
    private float max;
    private float min;

    public void rounds() {
        this.sum = (float) ((float) Math.round(sum * 100) / 100.00);
        this.avg = (float) ((float) Math.round(avg * 100) / 100.00);
        this.max = (float) ((float) Math.round(max * 100) / 100.00);
        this.min = (float) ((float) Math.round(min * 100) / 100.00);
    }
}

