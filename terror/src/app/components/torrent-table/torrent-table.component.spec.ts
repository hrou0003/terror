import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TorrentTableComponent } from './torrent-table.component';

describe('TorrentTableComponent', () => {
  let component: TorrentTableComponent;
  let fixture: ComponentFixture<TorrentTableComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TorrentTableComponent],
    }).compileComponents();

    fixture = TestBed.createComponent(TorrentTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
