import { ComponentFixture, TestBed } from '@angular/core/testing';
import TorrentDashboardPage from './torrent-dashboard.page';

describe('TorrentDashboardComponent', () => {
  let component: TorrentDashboardPage;
  let fixture: ComponentFixture<TorrentDashboardPage>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [TorrentDashboardPage],
    }).compileComponents();

    fixture = TestBed.createComponent(TorrentDashboardPage);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
