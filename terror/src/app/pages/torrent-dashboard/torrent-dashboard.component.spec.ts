import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TorrentDashboardComponent } from './torrent-dashboard.component';

describe('TorrentDashboardComponent', () => {
  let component: TorrentDashboardComponent;
  let fixture: ComponentFixture<TorrentDashboardComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TorrentDashboardComponent],
    }).compileComponents();

    fixture = TestBed.createComponent(TorrentDashboardComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
